(ns jepsen.rabbitmq
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.java.io       :as io]
            [jepsen.core           :as core]
            [jepsen.util           :refer [meh timeout log-op]]
            [jepsen.codec          :as codec]
            [jepsen.core           :as core]
            [jepsen.control        :as c]
            [jepsen.control.util   :as cu]
            [jepsen.client         :as client]
            [jepsen.db             :as db]
            [jepsen.generator      :as gen]
            [knossos.core          :as knossos]
            [knossos.op            :as op]
            [langohr.core          :as rmq]
            [langohr.channel       :as lch]
            [langohr.confirm       :as lco]
            [langohr.queue         :as lq]
            [langohr.exchange      :as le]
            [langohr.basic         :as lb])
  (:import (com.rabbitmq.client AlreadyClosedException
                                ShutdownSignalException)))

(def db
  (reify db/DB
    (setup! [_ test node]
      (c/cd "/tmp"
            (let [version "3.5.6"
                  file (str "rabbitmq-server_" version "-1_all.deb")]
              (when-not (cu/exists? file)
                (info "Fetching deb package")
                (c/exec :wget (str "http://www.rabbitmq.com/releases/rabbitmq-server/v" version "/" file)))

              (c/su
                ; Install package
                (try (c/exec :dpkg-query :-l :rabbitmq-server)
                     (catch RuntimeException _
                       (info "Installing rabbitmq")
                       (c/exec :apt-get :install :-y :erlang-nox)
                       (c/exec :dpkg :-i file)))

                ; Set cookie
                (when-not (and
                           (cu/exists? "/var/lib/rabbitmq/.erlang.cookie")
                           (= "jepsen-rabbitmq"
                             (c/exec :cat "/var/lib/rabbitmq/.erlang.cookie")))
                  (info "Setting cookie")
                  (c/exec :service :rabbitmq-server :stop)
                  (c/exec :echo "jepsen-rabbitmq"
                          :> "/var/lib/rabbitmq/.erlang.cookie"))

                ; Update config
                (info "uploading config")
                (c/exec :echo
                        (-> "rabbitmq/rabbitmq.config" io/resource slurp)
                        :> "/etc/rabbitmq/rabbitmq.config")

                ; Ensure node is running
                (try (c/exec :service :rabbitmq-server :status)
                     (catch RuntimeException _
                     (info "Starting rabbitmq")
                     (c/exec :service :rabbitmq-server :start)))

                ; Prepare for cluster join
                (when-not (= node (first (:nodes test)))
                  (c/exec :rabbitmqctl :stop_app))

                ; Wait for everyone to start up
                (core/synchronize test)

                ; Join
                (let [p (core/primary test)]
                  (when-not (= node p)
                    (info node "joining" p)
                    (c/exec :rabbitmqctl :join_cluster (str "rabbit@" (name p)))
                    (info node "joined" p)
                    (c/exec :rabbitmqctl :start_app)
                    (info node "started")))

                ; Use mirroring
                (core/synchronize test)
                (info node "Enabling mirroring")
                (c/exec :rabbitmqctl :set_policy :ha-maj "jepsen."
                        "{\"ha-mode\": \"exactly\",
                          \"ha-params\": 3,
                          \"ha-sync-mode\": \"automatic\"}")

                (info node "Rabbit ready")))))

    (teardown! [_ test node]
      (c/su
;        (info "Stopping rabbitmq")
;        (meh (c/exec :rabbitmqctl :stop_app))
;        (meh (c/exec :rabbitmqctl :force_reset))
;        (meh (c/exec :service :rabbitmq-server :stop))
        (info node "Nuking rabbit")
        (meh (c/exec :killall :-9 "beam.smp" "epmd"))
        (c/exec :rm :-rf "/var/lib/rabbitmq/mnesia/")
        (c/exec :service :rabbitmq-server :stop)
        (info node "Rabbit dead")))))

(def queue "jepsen.queue")

(defn dequeue!
  "Given a channel and an operation, dequeues a value and returns the
  corresponding operation."
  [ch op]
  ; Rabbit+Langohr's auto-ack dynamics mean that even if we issue a dequeue req
  ; then crash, the message should be re-delivered and we can count this as a
  ; failure.
  (timeout 5000 (assoc op :type :fail :error :timeout)
           (let [[meta payload] (lb/get ch queue)
                 value          (codec/decode payload)]
             (if (nil? meta)
               (assoc op :type :fail :error :empty)
               (assoc op :type :ok :value value)))))

(defmacro with-ch
  "Opens a channel on 'conn for body, binds it to the provided symbol 'ch, and
  ensures the channel is closed after body returns."
  [[ch conn] & body]
  `(let [~ch (lch/open ~conn)]
     (try ~@body
          (finally
            (try (rmq/close ~ch)
                 (catch AlreadyClosedException _#))))))

(defrecord QueueClient [conn]
  client/Client
  (open! [client test node]
    (let [conn (rmq/connect {:host (name node)})]
      (assoc client :conn conn)
      (QueueClient. conn)))

  (setup! [client test]
    (with-ch [ch conn]
      (lq/declare ch queue
        :durable     true
        :auto-delete false
        :exclusive   false)))

  (teardown! [client test]
    (meh (with-ch [ch conn]
           (lq/purge ch queue))))

  (close! [client test]
    (meh (rmq/close conn)))

  (invoke! [client test op]
    (with-ch [ch conn]
      (case (:f op)
        :enqueue (do
                   (lco/select ch) ; Use confirmation tracking

                   ; Empty string is the default exhange
                   (lb/publish ch "" queue
                               (codec/encode (:value op))
                               :content-type  "application/edn"
                               :mandatory     true
                               :persistent    true)

                   ; Block until message acknowledged
                   (if (lco/wait-for-confirms ch 5000)
                     (assoc op :type :ok)
                     (assoc op :type :fail)))

        :dequeue (dequeue! ch op)

        :drain (loop [values []]
                 (let [v (dequeue! ch op)]
                  (if (= (:type v) :ok)
                    (recur (conj values (:value v)))
                    (assoc op :type :ok, :value values))))))))

(defn queue-client [] (QueueClient. nil))

; https://www.rabbitmq.com/blog/2014/02/19/distributed-semaphores-with-rabbitmq/
; enqueued is shared state for whether or not we enqueued the mutex record
; held is independent state to store the currently held message
(defrecord Semaphore [enqueued? conn ch tag]
  client/Client
  (setup! [_ test node]
    (let [conn (rmq/connect {:host (name node)})]
      (with-ch [ch conn]
        (lq/declare ch "jepsen.semaphore"
                    :durable true
                    :auto-delete false
                    :exclusive false)

        ; Enqueue a single message
        (when (compare-and-set! enqueued? false true)
          (lco/select ch)
          (lq/purge ch "jepsen.semaphore")
          (lb/publish ch "" "jepsen.semaphore" (byte-array 0))
          (when-not (lco/wait-for-confirms ch 5000)
            (throw (RuntimeException.
                     "couldn't enqueue initial semaphore message!")))))

      (Semaphore. enqueued? conn (atom (lch/open conn)) (atom nil))))

  (teardown! [_ test]
    ; Purge
    (meh (timeout 5000 nil
                  (with-ch [ch conn]
                    (lq/purge ch "jepsen.semaphore"))))
    (meh (rmq/close @ch))
    (meh (rmq/close conn)))

  (invoke! [this test op]
    (case (:f op)
      :acquire (locking tag
                 (if @tag
                   (assoc op :type :fail :value :already-held)

                   (timeout 5000 (assoc op :type :fail :value :timeout)
                      (try
                        ; Get a message but don't acknowledge it
                        (let [dtag (-> (lb/get @ch "jepsen.semaphore" false)
                                       first
                                       :delivery-tag)]
                          (if dtag
                            (do (reset! tag dtag)
                                (assoc op :type :ok :value dtag))
                            (assoc op :type :fail)))

                        (catch ShutdownSignalException e
                          (meh (reset! ch (lch/open conn)))
                          (assoc op :type :fail :value (.getMessage e)))

                        (catch AlreadyClosedException e
                          (meh (reset! ch (lch/open conn)))
                          (assoc op :type :fail :value :channel-closed))))))

      :release (locking tag
                 (if-not @tag
                   (assoc op :type :fail :value :not-held)
                   (timeout 5000 (assoc op :type :ok :value :timeout)
                            (let [t @tag]
                              (reset! tag nil)
                              (try
                                ; We're done now--we try to reject but it
                                ; doesn't matter if we succeed or not.
                                (lb/reject @ch t true)
                                (assoc op :type :ok)

                                (catch AlreadyClosedException e
                                  (meh (reset! ch (lch/open conn)))
                                  (assoc op :type :ok :value :channel-closed))

                                (catch ShutdownSignalException e
                                  (assoc op
                                         :type :ok
                                         :value (.getMessage e)))))))))))

(defn mutex [] (Semaphore. (atom false) nil nil nil))
