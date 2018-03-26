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
  [test node]
  (if (not= "faunadb stop/waiting"
            (c/exec :initctl :status :faunadb))
    (info node "FaunaDB already running.")
    (do (info node "Starting FaunaDB...")
        (c/su
         (c/exec :initctl :start :faunadb)
         (Thread/sleep 30000)
         (jepsen/synchronize test)

         (when (= node (jepsen/primary test))
           (info node "initializing FaunaDB cluster")
           (c/exec :faunadb-admin :init)
           (Thread/sleep 10000)))
        (jepsen/synchronize test)

        (when (not= node (jepsen/primary test))
          (info node "joining FaunaDB cluster")
          (c/exec :faunadb-admin :join (jepsen/primary test))
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
  [test node partitions]
  (vals
   (group-by (fn [n]
               (mod (+ 1 (.indexOf (:nodes test) n)) partitions))
             (:nodes test))))

(defn configure!
  "Configure FaunaDB."
  [test node partitions]
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
              :network_datacenter_name (str/join ["replica-" (.indexOf (:nodes test) node)])
              :network_host_id node
              :network_listen_address node
              :storage_transaction_log_nodes (log-configuration test node partitions)}))
            :> "/etc/faunadb.yml")))

(defn teardown!
  "Gracefully stops FaunaDB and removes data files"
  [test node]
  (stop! test node)
  (debian/uninstall! :faunadb)
  (c/exec :bash :-c "rm -rf /var/lib/faunadb/*")
  (info node "FaunaDB removed"))
