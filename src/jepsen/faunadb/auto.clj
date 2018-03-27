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

(defn start!
  "Start faunadb on node."
  [test node replicas]
  (if (not= "faunadb stop/waiting"
            (c/exec :initctl :status :faunadb))
    (info node "FaunaDB already running.")
    (do (info node "Starting FaunaDB...")
        (c/su
          (c/exec :initctl :start :faunadb))
        (Thread/sleep 30000)
        (jepsen/synchronize test)

        (when (= node (jepsen/primary test))
          (info node "initializing FaunaDB cluster")
          (c/exec :faunadb-admin :init)
          (Thread/sleep 10000))
        (jepsen/synchronize test)

        (when (not= node (jepsen/primary test))
          (info node "joining FaunaDB cluster")
          (c/exec :faunadb-admin :join (jepsen/primary test))
          (Thread/sleep 10000))
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
          (Thread/sleep 10000))
        (jepsen/synchronize test)

        (info node "FaunaDB started")))
  :started)

(defn kill!
  "Kills FaunaDB on node."
  [test node]
  (util/meh (c/su (cu/grepkill! "faunadb.yml")))
  (info node "FaunaDB killed.")
  :killed)

(defn stop!
  "Gracefully stops FaunaDB on a node."
  [test node]
  (c/su (c/exec :initctl :stop :faunadb))
  (info node "FaunaDB stopped")
  :stopped)

(def repo-key
  "FaunaDB dpkg repository key."
  "TPwTIfv9rYCBsY9PR2Y31F1X5JEUFIifWopdM3RvdHXaLgjkOl0wPoNp1kif1hJS")

(defn install!
  "Install a particular version of FaunaDB."
  [version]
  (debian/install-jdk8!)
  (debian/add-repo! "faunadb"
                    (str/join ["deb [arch=all] https://" repo-key "@repo.fauna.com/enterprise/debian unstable non-free"]))
  (c/su (c/exec :wget :-qO :- "https://repo.fauna.com/faunadb-gpg-public.key" |
                :apt-key :add :-))
  (debian/install {"faunadb" version}))

(defn log-configuration
  "Configure the transaction log for the current topology."
  [test node replicas]
  (if (= 1 replicas)
    [[(jepsen/primary :test)]]
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
   (c/exec :echo (-> "faunadb.conf"
                     io/resource
                     slurp)
           :> "/etc/init/faunadb.conf")
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
              :storage_transaction_log_nodes (log-configuration test node replicas)}))
            :> "/etc/faunadb.yml")))

(defn teardown!
  "Gracefully stops FaunaDB and removes data files"
  [test node]
  (stop! test node)
  (debian/uninstall! :faunadb)
  (c/exec :bash :-c "rm -rf /var/lib/faunadb/*")
  (c/exec :bash :-c "rm -rf /var/log/faunadb/*")
  (info node "FaunaDB removed"))
