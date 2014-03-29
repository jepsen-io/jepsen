(ns jepsen.system.rabbitmq
  (:require [jepsen.core          :as core]
            [jepsen.control       :as c]
            [jepsen.control.util  :as cu]
            [jepsen.db            :as db]
            [clojure.tools.logging :refer [debug info warn]]))

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

                (core/synchronize test)
                (info (c/exec :rabbitmqctl :cluster_status))

                (info node "Rabbit ready")))))

    (teardown! [_ test node]
      (c/su
        (info "Stopping rabbitmq")
        (c/exec :rabbitmqctl :stop_app)
        (c/exec :rabbitmqctl :force_reset)
        (c/exec :service :rabbitmq-server :stop)
        (info "Rabbit stopped")))))
