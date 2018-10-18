(ns jepsen.faunadb.auto
  "FaunaDB automation functions, for starting, stopping, etc."
  (:require [clj-yaml.core :as yaml]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [jepsen [core :as jepsen]
                    [client :as client]
                    [util :as util]
                    [control :as c :refer [|]]]
            [jepsen.control.net :as cn]
            [jepsen.faunadb.client :as f]
            [jepsen.os.debian :as debian]
            [jepsen.control.util :as cu]))

(def data-dir
  "Where does FaunaDB store data files?"
  "/var/lib/faunadb")

(def log-dir
  "Directory for fauna logs"
  "/var/log/faunadb")

; FaunaDB setup is really slow, and we haven't been able to get tests to launch
; in under 8 minutes or so. To work around that, we're gonna do something kinda
; ugly: cache the data files from a clean cluster in a local directory, and
; copy them into place to speed up initial convergence.

(def cache-dir
  "A directory used to cache the database state"
  "/tmp/jepsen/faunadb-cache")

(def cache-equivalency-keys
  "What keys from a test define whether two caches are equivalent?"
  [:version
   :nodes
   :replicas])

(defn cache-equivalent?
  "Are two tests equivalent for purposes of the cache?"
  [test-1 test-2]
  (let [a (select-keys test-1 cache-equivalency-keys)
        b (select-keys test-2 cache-equivalency-keys)]
    (info "cache equivalent?" a b)
    (= a b)))

(defn cached-test
  "Returns the test map cached on the current node, or nil if no cache exists."
  []
  (try (edn/read-string (c/exec :cat (str cache-dir "/test.edn")))
       (catch RuntimeException e)))

(defn cache-valid?
  "Do we have a cache that would work for this test?"
  [test]
  (cache-equivalent? test (cached-test)))

(defn clear-cache!
  "Wipe out the cache on this node"
  []
  (info "Clearing FaunaDB data cache")
  (c/su
    (c/exec :rm :-rf cache-dir)))

(defn build-cache!
  "Builds a cached copy of the database state by tarring up all the files in
  /var/lib/faunadb, and adding our test map. Wipes out any existing cache."
  [test]
  (info "Building FaunaDB data cache")
  (clear-cache!)
  (c/su
    (c/exec :mkdir :-p cache-dir)
    (c/exec :cp :-a data-dir (str cache-dir "/data"))
    (c/exec :echo (with-out-str
                    (pprint (select-keys test cache-equivalency-keys)))
            :> (str cache-dir "/test.edn"))))

(defn unpack-cache!
  "Replaces Fauna's data files with the cache"
  []
  (info "Unpacking cached FaunaDB data files")
  (c/su
    (c/exec :rm :-rf data-dir)
    (c/exec :cp :-a (str cache-dir "/data") data-dir)))

(defn replicas
  "Returns a list of all the replicas for a given test, e.g. \"replica-0\",
  \"replica-1\", ..."
  [test]
  (->> test :replicas range (map (partial str "replica-"))))

(defn replica
  "Returns the index of the replica to which the node belongs. Tests have a
  :replica key, which indicates the number of replicas we would like to have."
  [test node]
  (let [replicas (:replicas test)]
    (str "replica-"
         (if (= 1 replicas)
           0
           (mod (.indexOf (:nodes test) node) replicas)))))

(defn nodes-by-replica
  "Constructs a map of replica names to the nodes in each replica."
  [test]
  (->> test :nodes (group-by (partial replica test))))

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
  [test node]
  (when (= node (jepsen/primary test))
    (info node "initializing FaunaDB cluster")
    (c/exec :faunadb-admin :init))
  (jepsen/synchronize test 300)

  (when (not= node (jepsen/primary test))
    (info node "joining FaunaDB cluster")
    (c/exec :faunadb-admin :join (jepsen/primary test)))
  (jepsen/synchronize test)

  (when (= node (jepsen/primary test))
    (info node (str/join ["creating " (:replicas test) " replicas"]))
    (when (< 1 (:replicas test))
      (c/exec :faunadb-admin
              :update-replication
              (replicas test)))
    (when (:wait-for-convergence test)
      (wait-for-replication node)
      (info node "Replication complete")))

  (jepsen/synchronize test 1200) ; this is slooooooowwww
  :initialized)

(defn status
  "Systemd status for FaunaDB"
  []
  (let [msg (try
              (c/su (c/exec :service :faunadb :status))
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
  (info "Installing faunadb")
  (c/su
    (info "Installing JDK")
    (debian/install-jdk8!)
    (info "Adding apt key")
    (c/exec :wget :-qO :- "https://repo.fauna.com/faunadb-gpg-public.key" |
            :apt-key :add :-)
    (info "Adding repo")
    (debian/add-repo! "faunadb"
                      "deb [arch=all] https://repo.fauna.com/debian stable non-free")
    (info "Install faunadb")
    (debian/install {"faunadb" (str (:version test) "-0")})
    (when-let [k (:datadog-api-key test)]
      (when-not (debian/installed? :datadog-agent)
        (info "Datadog install")
        (c/exec (str "DD_API_KEY=" k)
                :bash :-c
                (c/lit "\"$(curl -L https://raw.githubusercontent.com/DataDog/datadog-agent/master/cmd/agent/install_script.sh)\""))))))

(defn log-configuration
  "Configuration for the transaction log for the current topology."
  [test node]
  ; TODO: why don't we provide multiple nodes when there's only one replica?
  (if (= 1 (:replicas test))
    [[(jepsen/primary test)]]
    ; We want a collection of log partitions; each partition is a list of
    ; nodes. Each partition should have one node from each replica. We build up
    ; partitions incrementally by pulling nodes off of the set of replicas.
    (loop [partitions []
           replicas (vals (nodes-by-replica test))]
      (if (some empty? replicas)
        ; We're out of nodes to assign
        partitions
        ; Make a new partition with the first node from each replica
        (recur (conj partitions (map first replicas))
               (map next replicas))))))

(defn configure!
  "Configure FaunaDB."
  [test node]
  (info "Configuring" node)
  (c/su
    (let [ip (cn/local-ip)]
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
                  {:auth_root_key                  f/root-key
                   :network_coordinator_http_address ip
                   :network_broadcast_address      node
                   :network_datacenter_name        (replica test node)
                   :network_host_id                node
                   :network_listen_address         ip
                   :storage_transaction_log_nodes  (log-configuration test node)}
                  (when (:datadog-api-key test)
                    {:stats_host "localhost"
                     :stats_port 8125})))
              :> "/etc/faunadb.yml"))))

(defn teardown!
  "Gracefully stops FaunaDB and removes data files"
  [test node]
  (when (debian/installed? :faunadb)
    (kill! test node)
    (stop! test node)
    ; (debian/uninstall! :faunadb)
    (c/su
      (c/exec :rm :-rf
              (c/lit (str data-dir "/*"))
              (c/lit (str log-dir "/*"))))
    (info node "FaunaDB torn down")))
