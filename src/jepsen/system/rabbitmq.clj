(ns jepsen.system.rabbitmq
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.core           :as core]
            [jepsen.util           :refer [meh]]
            [jepsen.codec          :as codec]
            [jepsen.core           :as core]
            [jepsen.control        :as c]
            [jepsen.control.util   :as cu]
            [jepsen.client         :as client]
            [jepsen.db             :as db]
            [jepsen.generator      :as gen]
            [knossos.core          :as knossos]
            [langohr.core          :as rmq]
            [langohr.channel       :as lch]
            [langohr.confirm       :as lco]
            [langohr.queue         :as lq]
            [langohr.exchange      :as le]
            [langohr.basic         :as lb])
  (:import (com.rabbitmq.client AlreadyClosedException)))

(def db
  (reify db/DB
    (setup! [_ test node]
      (c/cd "/tmp"
            (let [file "rabbitmq-server_3.2.4-1_all.deb"]
              (when-not (cu/file? file)
                (info "Fetching deb package")
                (c/exec :wget (str "http://www.rabbitmq.com/releases/rabbitmq-server/v3.2.4/" file)))

              (c/su
                ; Install package
                (try (c/exec :dpkg-query :-l :rabbitmq-server)
                     (catch RuntimeException _
                       (info "Installing rabbitmq")
                       (c/exec :apt-get :install :-y :erlang-nox)
                       (c/exec :dpkg :-i file)))

                ; Set cookie
                (when-not (= "jepsen-rabbitmq"
                             (c/exec :cat "/var/lib/rabbitmq/.erlang.cookie"))
                  (info "Setting cookie")
                  (c/exec :service :rabbitmq-server :stop)
                  (c/exec :echo "jepsen-rabbitmq"
                          :> "/var/lib/rabbitmq/.erlang.cookie"))

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
                          \"ha-params\": 2,
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
        (info node "Rabbit dead")))))

(def queue "jepsen.queue")

(defn dequeue!
  "Given a channel and an operation, dequeues a value and returns the
  corresponding operation."
  [ch op]
  (let [[meta payload] (lb/get ch queue)
        value          (codec/decode payload)]
    (if (nil? meta)
      (assoc op :type :fail :value :exhausted)
      (assoc op :type :ok :value value))))

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
  (setup! [_ test node]
    (let [conn (rmq/connect {:host (name node)})]
      (with-ch [ch conn]
        ; Initialize queue
        (lq/declare ch queue
                    :durable     true
                    :auto-delete false
                    :exclusive   false))

      ; Return client
      (QueueClient. conn)))

  (teardown! [_ test]
    ; Purge
    (meh (with-ch [ch conn]
           (lq/purge ch queue)))

    ; Close
    (meh (rmq/close conn)))

  (invoke! [this test op]
    (with-ch [ch conn]
      (case (:f op)
        :enqueue (do
                   (lco/select ch) ; Use confirmation tracking

                   ; Empty string is the default exhange
                   (lb/publish ch "" queue
                               (codec/encode (:value op))
                               :content-type "application/edn"
                               :persistent true)

                   ; Block until message acknowledged
                   (if (lco/wait-for-confirms ch 5000)
                     (assoc op :type :ok)
                     (assoc op :type :fail)))

        :dequeue (dequeue! ch op)

        :drain   (do
                   ; Note that this does more dequeues than strictly necessary
                   ; owing to lazy sequence chunking.
                   (->> (repeat op)                  ; Explode drain into
                        (map #(assoc % :f :dequeue)) ; infinite dequeues, then
                        (map (partial dequeue! ch))  ; dequeue something
                        (take-while knossos/ok?)     ; as long as stuff arrives,
                        (interleave (repeat op))     ; interleave with invokes
                        (drop 1)                     ; except the initial one
                        (map (partial core/conj-op! test))
                        dorun)
                   (assoc op :type :info :value :exhausted))))))

(defn queue-client [] (QueueClient. nil))
