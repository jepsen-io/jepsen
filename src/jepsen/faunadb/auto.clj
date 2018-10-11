(ns jepsen.faunadb.auto
  "FaunaDB automation functions, for starting, stopping, etc."
  (:require [clj-yaml.core :as yaml]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen [core :as jepsen]
                    [client :as client]
                    [util :as util]
                    [control :as c :refer [|]]]
            [jepsen.faunadb.client :as f]
            [jepsen.os.debian :as debian]
            [jepsen.control.util :as cu]))

(defn wait-for-replication
  "Blocks until local node has completed data movement."
  [node]
  (let [mvmnt (c/exec :faunadb-admin :movement-status)]
    (when (not= (last (str/split-lines mvmnt)) "No data movement is currently in progress.")
        (info mvmnt)
        (Thread/sleep 5000)
        (recur node))))

(defn init!
  "Sets up cluster on node. Must be called on all nodes in test concurrently."
  [test node replicas]
  (when (= node (jepsen/primary test))
    (info node "initializing FaunaDB cluster")
    (c/exec :faunadb-admin :init))
  (jepsen/synchronize test 300)

  (when (not= node (jepsen/primary test))
    (info node "joining FaunaDB cluster")
    (c/exec :faunadb-admin :join (jepsen/primary test)))
  (jepsen/synchronize test)

  (when (= node (jepsen/primary test))
    (info node (str/join ["creating " replicas " replicas"]))
    (if (not= 1 replicas)
      (c/exec :faunadb-admin
              :update-replication
              (mapv
               (fn [i]
                 (str/join ["replica-" i]))
               (range replicas))))
    (when (:wait-for-convergence test)
      (wait-for-replication node)
      (info node "Replication complete")))

  (jepsen/synchronize test 600) ; this is slooooooowwww
  :initialized)

(defn status
  "Systemd status for FaunaDB"
  []
  (let [msg (try
              (c/exec :service :faunadb :status)
              (catch RuntimeException e
                (let [m (.getMessage e)]
                  (if (re-find #"returned non-zero exit status 3" m)
                    m
                    (throw e)))))
        [_ state sub] (re-find #"Active: (\w+) \(([^\)]+)\)\s" msg)]
    (when-not (and state sub)
      (throw (RuntimeException. (str "Not sure how to interpret service status:\n"
                                     msg))))
    [state sub]))

(defn running?
  "Is Fauna running?"
  []
  ; I forget how many states systemd has so uhhh let's be conservative
  (let [[state sub] (status)
        running? (case state
                   "active" (case sub
                              "running" true
                              nil)
                   "activating" (case sub
                                  "auto-restart" true
                                  nil)
                   "failed"   (case sub
                                "Result: signal" false
                                nil)
                   "inactive" (case sub
                                "dead" false
                                nil)
                   nil)]
    (when (nil? running?)
      (throw
        (RuntimeException. (str "Don't know how to interpret status "
                                state " (" sub ")"))))
    running?))

(defn start!
  "Starts faunadb on node, if it is not already running"
  [test node]
  (if (running?)
    (info node "FaunaDB already running.")
    (do (info node "Starting FaunaDB...")
        (c/su (c/exec :service :faunadb :start))
        (c/exec
          :bash :-c "while ! netstat -tna | grep 'LISTEN\\>' | grep -q ':8444\\>'; do sleep 0.1; done")
        (info node "FaunaDB started")))
  :started)

(defn kill!
  "Kills FaunaDB on node."
  [test node]
  (util/meh (c/su (cu/grepkill! "faunadb.jar")))
  (info node "FaunaDB killed.")
  :killed)

(defn stop!
  "Gracefully stops FaunaDB on a node."
  [test node]
  (c/su (c/exec :service :faunadb :stop))
  (info node "FaunaDB stopped")
  :stopped)

(defn install!
  "Install a particular version of FaunaDB."
  [test]
  (c/su
    (debian/install-jdk8!)
    (debian/add-repo! "faunadb"
                      "deb [arch=all] https://repo.fauna.com/debian stable non-free")
    (c/exec :wget :-qO :- "https://repo.fauna.com/faunadb-gpg-public.key" |
            :apt-key :add :-)
    (debian/update!)
    (debian/install {"faunadb" (str (:version test) "-0")})
    (when-let [k (:datadog-api-key test)]
      (c/exec (str "DD_API_KEY=" k)
              :bash :-c
              (c/lit "\"$(curl -L https://raw.githubusercontent.com/DataDog/datadog-agent/master/cmd/agent/install_script.sh)\"")))))

; TODO: clarify exactly what modulo logic is going on for logs and replicas.
; How are clusters laid out?
(defn log-configuration
  "Configure the transaction log for the current topology."
  [test node replicas]
  (if (= 1 replicas)
    [[(jepsen/primary test)]]
    (vals
     (group-by (fn [n]
                 (mod (+ 1 (.indexOf (:nodes test) n)) replicas))
               (:nodes test)))))

(defn replica
  "Returns the index of the replica to which the node belongs."
  [test node replicas]
  (if (= 1 replicas)
    0
    (mod (.indexOf (:nodes test) node) replicas)))

(defn configure!
  "Configure FaunaDB."
  [test node replicas]
  (info "Configuring" node)
  (c/su
    ; Systemd init file
   ;(c/exec :echo (-> "faunadb.conf"
   ;                  io/resource
   ;                  slurp)
   ;        :> "/etc/init/faunadb.conf")
   ;(c/exec :systemctl :daemon-reload)

   ; Defaults
   (c/exec :echo (-> "faunadb.defaults" io/resource slurp)
           :> "/etc/default/faunadb")

   ; Fauna config
   (c/exec :echo
           (yaml/generate-string
            (merge
             (yaml/parse-string (-> "faunadb.yml"
                                    io/resource
                                    slurp))
             {:auth_root_key f/root-key
              :network_coordinator_http_address node
              :network_broadcast_address node
              :network_datacenter_name (str/join ["replica-" (replica test node replicas)])
              :network_host_id node
              :network_listen_address node
              :storage_transaction_log_nodes (log-configuration test node replicas)}
             (when (:datadog-api-key test)
               {:stats_host "localhost"
                :stats_port 8125})))
            :> "/etc/faunadb.yml")))

(defn teardown!
  "Gracefully stops FaunaDB and removes data files"
  [test node]
  (when (debian/installed? :faunadb)
    (kill! test node)
    (stop! test node)
    ; (debian/uninstall! :faunadb)
    (c/su
      (c/exec :bash :-c "rm -rf /var/lib/faunadb/*")
      (c/exec :bash :-c "rm -rf /var/log/faunadb/*"))
    (info node "FaunaDB torn down")))
