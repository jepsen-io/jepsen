(ns jepsen.faunadb.auto
  "FaunaDB automation functions, for starting, stopping, etc."
  (:require [clj-yaml.core :as yaml]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [slingshot.slingshot :refer [try+ throw+]]
            [dom-top.core :refer [with-retry]]
            [jepsen [core :as jepsen]
                    [client :as client]
				            [db :as db]
                    [util :as util]
                    [control :as c :refer [|]]]
            [jepsen.control.net :as cn]
            [jepsen.faunadb [client :as f]
                            [topology :as topo]]
            [jepsen.os.debian :as debian]
            [jepsen.control.util :as cu])
  (:import (java.util.concurrent CountDownLatch)))

(def data-dir
  "Where does FaunaDB store data files?"
  "/var/lib/faunadb")

(def log-dir
  "Directory for fauna logs"
  "/var/log/faunadb")

(def log-files
  "Files to snarf at the end of each test"
  ["/var/log/faunadb/core.log"
   "/var/log/faunadb/query.log"
   "/var/log/faunadb/exception.log"])

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

(defn wait-for-replication
  "Blocks until local node has completed data movement."
  [node]
  (let [mvmnt (c/exec :faunadb-admin :movement-status)]
    (when (not= (last (str/split-lines mvmnt)) "No data movement is currently in progress.")
        (info mvmnt)
        (Thread/sleep 5000)
        (recur node))))

(defn join!
  "Joins this node to the given target node."
  [replica target]
  (c/su
    (info "joining FaunaDB node" target)
    (c/exec :faunadb-admin :join :-r replica target)))

(defn init!
  "Sets up cluster on node. Must be called on all nodes in test concurrently."
  [test replica node]
  (when (= node (jepsen/primary test))
    (info node "initializing FaunaDB cluster")
    (c/exec :faunadb-admin :init :-r replica))
  (jepsen/synchronize test 300)

  (when (not= node (jepsen/primary test))
    (join! replica (jepsen/primary test))
    (c/exec :faunadb-admin :join :-r replica (jepsen/primary test)))
  (jepsen/synchronize test)

  (when (= node (jepsen/primary test))
    (info node (str/join ["creating " (:replicas test) " replicas"]))
    (when (< 1 (:replica-count @(:topology test)))
      (c/exec :faunadb-admin
              :update-replica
              :data+log
              (topo/replicas @(:topology test))))
    (when (:wait-for-convergence test)
      (wait-for-replication node)
      (info node "Replication complete")))

  (jepsen/synchronize test 2000) ; this is slooooooowwww
  :initialized)

(defn host-id
  "The internal Fauna host ID for the given node, or the local node"
  ([]
   (c/su (c/exec :faunadb-admin :show-identity)))
  ([node]
   (c/su (c/exec :faunadb-admin :host-id node))))

(defn systemd-status
  "Systemd status for FaunaDB"
  []
  (let [msg (try+
              (c/su (c/exec :service :faunadb :status))
              (catch [:type :jepsen.control/nonzero-exit :exit 3] {:keys [out]}
                  out))

        [_ state sub] (re-find #"Active: (\w+) \(([^\)]+)\)\s" msg)]
    (when-not (and state sub)
      (throw (RuntimeException. (str "Not sure how to interpret service status:\n"
                                     msg))))
    [state sub]))

(defn running?
  "Is Fauna running?"
  []
  ; I forget how many states systemd has so uhhh let's be conservative
  (let [[state sub] (systemd-status)
        running? (case state
                   "active" (case sub
                              "running" true
                              nil)
                   "activating" (case sub
                                  "auto-restart" true
                                  nil)
                   "deactivating" (case sub
                                    "stop-sigterm" false
                                    nil)
                   "failed"   (case sub
                                "Result: signal" false
                                "Result: timeout" false
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

(defn parse-percent
  "Takes a string like 34.2% and returns 0.342"
  [s]
  (/ (Double/parseDouble (subs s 0 (dec (.length s)))) 100))

(defn status
  "FaunaDB admin status: a collection of nodes, each of which is a map of

  :replica      e.g. replica-1
  :status       e.g. :up
  :state        e.g. :live
  :worker-id    e.g. 515
  :log-segment  e.g. Segment-1
  :address      Node name, e.g. n1
  :owns         String (for now) fraction
  :goal         String (for now) fraction
  :host-id      Unique internal identifier, e.g. 0e7a7ec4-94d4-444f-af0f...

  `nil`, if FaunaDB isn't running."
  []
  (when (running?)
    (->> (c/su (c/exec :faunadb-admin :status))
         str/split-lines
         (reduce
           (fn [[state replica replicas] line]
             ; Blank lines put is in a ready-for-dc state
             (if (re-find #"\A\s*\Z" line)
               [:ready nil replicas]

               (case state
                 :fresh (if (re-find #"\ALoaded configuration" line)
                          [:ready nil replicas]
                          (throw+ {:type :parse-error, :state state, :line line}))
                 :ready (let [[m _ replica] (re-find #"\A(Replica|Datacenter): ([^ ]+?) " line)]
                          (when-not replica
                            (throw+
                              {:type :parse-error, :state state, :line line}))
                          [:=== replica replicas])
                 :===   (if (re-find #"\A===+\Z" line)
                          [:headers replica replicas]
                          (throw+ {:type :parse-error :state state, :line line}))
                 :headers (let [split (-> line
                                          ; This is the cleanest way I can
                                          ; figure out to deal with the presence
                                          ; of a space in this field, but no
                                          ; other field name
                                          (str/replace "Log Segment" "LogSegment")
                                          (str/split #"\s+"))]
                            (if (= split ["Status" "State" "WorkerID"
                                          "LogSegment" "Address" "Owns" "Goal"
                                          "HostID"])
                              [:node replica replicas]
                              (throw+ {:type :parse-error,
                                       :state state,
                                       :line line})))
                 :node    [:node replica
                           (conj replicas
                                 (-> [:status :state :worker-id :log-segment
                                      :address :owns :goal :host-id]
                                     (zipmap (str/split line #"\s+"))
                                     (update :status keyword)
                                     (update :state keyword)
                                     (update :owns parse-percent)
                                     (update :goal parse-percent)
                                     (update :worker-id #(Long/parseLong %))
                                     (assoc :replica replica)))])))
           [:fresh nil []])
         last)))

(defn wait-for-node-removal
  "Blocks until the given node is no longer a part of the cluster."
  [node]
  (with-retry []
    (when-let [n (first (filter #(= node (:address %)) (status)))]
      (info "Waiting for" node "to leave cluster:" (pr-str (:owns n)))
      (Thread/sleep 5000)
      (retry))
    (catch RuntimeException e
      (info "Couldn't get faunadb status:" e)
      (Thread/sleep 5000)
      (retry))))

(defn remove-node!
  "Removes a node from the cluster. Takes a node name and looks up its host
  ID. Returns a delay which waits for the node to actually leave."
  [node]
  (info "Removing" node "from cluster")
  (c/su (c/exec :faunadb-admin :remove (c/exec :faunadb-admin :host-id node)))
  (delay (wait-for-node-removal node)))

(defn status->topology
  "Converts a status map to a topology."
  [status]
  (assert status)
  ; (info "Status is" (with-out-str (pprint status)))
  {:replica-count (count (distinct (map :replica status)))
   :nodes         (->> status
                       (remove #(= :removed (:state %)))
                       (mapv (fn [node]
                               {:node (:address node)
                                :state (condp = (:state node)
                                         :live     :active
                                         :removed  :removed
                                         (:state node))
                                :replica (:replica node)
                                :log-part (if (= "none" (:log-segment node))
                                            nil
                                            (-> (re-find #"(\d+)$"
                                                         (:log-segment node))
                                                (get 1)
                                                (Long/parseLong)))})))})

(defn refresh-topology!
  "Reloads the topology based on what some randomly selected node thinks it
  is. Oh, this could go so wrong."
  [test]
  (info "Refreshing topology")
  (let [s (promise)]
    (future
      (try
        (let [latch (CountDownLatch. (count (:nodes test)))]
          (c/on-nodes test (fn [test node]
                             (when-let [status (status)]
                               (deliver s status))
                             ; Wait for everyone to have had a chance to read
                             ; their node's status. If nobody got a status, we
                             ; fill in nil, and that lets the caller move on.
                             (.countDown latch)
                             (.await latch)
                             (deliver s nil))))
        (catch Throwable t
          ; We expect this to occur during reboots and such
          ; (warn t "Thrown during topology refresh")))
          )))
    (if-let [status @s]
      (let [topology (status->topology status)]
        (reset! (:topology test) topology)
        (info "New topology is" (with-out-str (pprint topology))))
      (info "Couldn't update topology; no node returned a status."))))

(defonce node-locks
  (util/named-locks))

(defn start!
  "Starts faunadb on node, if it is not already running"
  [test node]
  (if (running?)
    (info node "FaunaDB already running.")
    (util/with-named-lock node-locks node
      (c/su (info node "Starting FaunaDB...")
            (c/exec :service :faunadb :start)
            ; Wait for admin interface to come up
            (while (not (try (c/exec :netstat :-tna |
                                     :grep "LISTEN\\>" |
                                     :grep :-q ":8444\\>")
                             (catch java.lang.RuntimeException e false)))
              (Thread/sleep 100))
            (c/exec :chmod :a+r log-files)
            (info node "FaunaDB started"))))
  :started)

(defn kill!
  "Kills FaunaDB on node."
  [test node]
  (util/with-named-lock node-locks node
    (c/su
      (util/meh (cu/grepkill! "faunadb.jar"))
      ; Don't restart!
      (c/exec :service :faunadb :stop))
    (info node "FaunaDB killed.")
    :killed))

(defn stop!
  "Gracefully stops FaunaDB on a node."
  [test node]
  (util/with-named-lock node-locks node
    (info node "Stopping FaunaDB")
    (c/su (c/exec :service :faunadb :stop))
    :stopped))

(defn delete-data-files!
  "Erases FaunaDB data files on a node, but leaves logs intact."
  []
  (info "Deleting data files")
  (c/su
    (c/exec :rm :-rf
            (c/lit (str data-dir "/*")))))

(defn install!
  "Install a particular version of FaunaDB."
  [test]
  (info "Installing faunadb")
  (c/su
    (info "Installing JDK")
    (debian/install-jdk11!)
    (info "Adding apt key")
    (c/exec :wget :-qO :- "https://repo.fauna.com/faunadb-gpg-public.key" |
            :apt-key :add :-)
    (info "Adding repo")
    (debian/add-repo! "faunadb-stable"
                      "deb [arch=all] https://repo.fauna.com/debian stable non-free")
    (debian/add-repo! "faunadb"
                      "deb [arch=all] https://repo.fauna.com/debian unstable non-free")
    (debian/maybe-update!)

    (let [v (:version test)]
      (assert v)
      (if (re-find #"\.deb$" v)
        ; Install deb file
        (do (debian/install ["bash-completion"])
            (c/exec :mkdir :-p "/tmp/jepsen")
            (c/exec :chmod "a+rwx" "/tmp/jepsen")
            (info "Uploading" v)
            (c/upload v "/tmp/jepsen/faunadb.deb")
            (info "Installing" v)
            (c/exec :dpkg :-i :--force-confnew "/tmp/jepsen/faunadb.deb"))
        (debian/install {"faunadb" (str (:version test) "-0")})))

    (when-let [k (:datadog-api-key test)]
      (when-not (debian/installed? :datadog-agent)
        (info "Datadog install")
        (c/exec (str "DD_API_KEY=" k)
                :bash :-c
                (c/lit "\"$(curl -L https://raw.githubusercontent.com/DataDog/datadog-agent/master/cmd/agent/install_script.sh)\""))))))

(defn configure!
  "Configure FaunaDB."
  [test topo node]
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
                   :network_host_id                node
                   :network_listen_address         ip}
                  (when (:accelerate-indexes test)
                    {:accelerate_indexes true})
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
    (delete-data-files!)
    (c/su (c/exec :rm :-rf (c/lit (str log-dir "/*"))))
    (info node "FaunaDB torn down")))

(defn db
  "FaunaDB DB"
  []
  (reify db/DB
    (setup! [_ test node]
      (install! test)
      (configure! test @(:topology test) node)
      (start! test node)
      (init! test (topo/replica @(:topology test) node) node))

    (teardown! [_ test node]
      (info node "tearing down FaunaDB")
      (teardown! test node))

    db/LogFiles
    (log-files [_ test node]
      log-files)))
