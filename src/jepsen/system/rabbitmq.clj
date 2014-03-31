(ns jepsen.system.rabbitmq
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.codec          :as codec]
            [jepsen.core           :as core]
            [jepsen.control        :as c]
            [jepsen.control.util   :as cu]
            [jepsen.client         :as client]
            [jepsen.db             :as db]
            [jepsen.generator      :as gen]
            [langohr.core          :as rmq]
            [langohr.channel       :as lch]
            [langohr.confirm       :as lco]
            [langohr.queue         :as lq]
            [langohr.exchange      :as le]
            [langohr.basic         :as lb]))

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
                    (c/exec :rabbitmqctl :start_app)))

                (info node "Rabbit ready")))))

    (teardown! [_ test node]
      (c/su
        (info "Stopping rabbitmq")
        (c/exec :rabbitmqctl :stop_app)
        (c/exec :rabbitmqctl :force_reset)
        (c/exec :service :rabbitmq-server :stop)
        (info "Rabbit stopped")))))

(def queue "jepsen.queue")

(defrecord QueueClient [conn ch]
  client/Client
  (setup! [_ test node]
    (let [conn (rmq/connect {:host (name node)})
          ch   (lch/open conn)]

      ; We want confirmation tracking on this connection
      (lco/select ch)

      ; Initialize queue
      (lq/declare ch queue
                  :durable     true
                  :auto-delete false
                  :exclusive   false)

      ; Return client
      (QueueClient. conn ch)))

  (teardown! [_ test]
    ; Purge
    (lq/purge ch queue)

    ; Close
    (rmq/close ch)
    (rmq/close conn))

  (invoke! [this test op]
    (case (:f op)
      :enqueue (do
                 ; Empty string is the default exhange
                 (lb/publish ch "" queue
                             (codec/encode (:value op))
                             :content-type "application/edn"
                             :persistent true)

                 ; Block until message acknowledged
                 (if (lco/wait-for-confirms ch 5000)
                   (assoc op :type :ok)
                   (assoc op :type :fail)))

      :dequeue (let [[meta payload] (lb/get ch queue)
                     value          (codec/decode payload)]
                 (if (nil? meta)
                   (assoc op :type :fail)
                   (assoc op :type :ok :value value))))))

(defn queue-client [] (QueueClient. nil nil))
