(ns aerospike.support
  "Core DB setup and support features"
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [dom-top.core :refer [with-retry letr]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis   :as nemesis]
                    [os        :as os]
                    [store     :as store]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.time :as nt]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [wall.hack])
  (:import (clojure.lang ExceptionInfo)
           (com.aerospike.client AerospikeClient
                                 AerospikeException
                                 AerospikeException$Connection
                                 AerospikeException$Timeout
                                 Bin
                                 Info
                                 Key
                                 Record)
           (com.aerospike.client.cluster Node)
           (com.aerospike.client.policy Policy
                                        ConsistencyLevel
                                        GenerationPolicy
                                        WritePolicy
                                        RecordExistsAction)))

(def local-package-dir
  "Local directory for Aerospike packages."
  "packages/")

(def remote-package-dir
  "/tmp/packages/")

(def ans "Aerospike namespace"
  "jepsen")

;;; asinfo helpers:
(defn kv-split
  [kv]
  (let [skv (str/split kv #"=" 2)]
    [(keyword (first skv)) (util/maybe-number (fnext skv))]))

(defn split-colons
  "Splits a colon-delimited string."
  [s]
  (str/split s #":"))

(defn split-commas
  "Splits a comma-delimited string."
  [s]
  (str/split s #","))

(defn split-semicolons
  "Splits a semicolon-delimited string."
  [s]
  (str/split s #";"))

(defn str-list-count
  [s]
  (count (split-commas s)))

;;; Client functions:
(defn nodes
  "Nodes known to a client."
  [^AerospikeClient client]
  (vec (.getNodes client)))

(defn server-info
  "Requests server info from a particular client node."
  ([^Node node, k]
   (Info/request nil node k))
  ([^Node node]
   (->> node
        (Info/request nil)
        (map (fn [[k v]]
               (let [k (keyword k)]
                 [k (case k
                      :features   (->> v split-semicolons (map keyword) set)
                      :statistics (->> (split-semicolons v)
                                       (map kv-split)
                                       (into (sorted-map)))
                      (util/maybe-number v))])))
        (into (sorted-map)))))

(defn close
  [^java.io.Closeable x]
  (.close x))

(defn create-client
  "Spinloop to create a client, since our hacked version will crash on init if
  it can't connect."
  [node]
  (with-retry [tries 30]
    (AerospikeClient. node 3000)
    (catch com.aerospike.client.AerospikeException e
      (when (zero? tries)
        (throw e))
      ; (info "Retrying client creation -" (.getMessage e))
      (Thread/sleep 1000)
      (retry (dec tries)))))

(defn connect
  "Returns a client for the given node. Blocks until the client is available."
  [node]
  ;; FIXME - client no longer blocks till cluster is ready.
  (let [client (create-client node)]
    ; Wait for connection
    (while (not (.isConnected client))
      (Thread/sleep 100))

    ; Wait for workable ops
    (while (try (server-info (first (nodes client)))
                false
                (catch AerospikeException$Timeout e
                  true))
      (Thread/sleep 10))
    client))

;;; asinfo commands:
(defn asadm-recluster!
  "Reclusters all connected nodes."
  []
  ;; Sends to all clustered nodes.
  (c/trace (c/su (c/exec :asadm :-e "asinfo -v recluster:"))))

(defn revive!
  "Revives a namespace on the local node."
  ([]
   (revive! ans))
  ([namespace]
   (c/su (c/exec :asinfo :-v (str "revive:namespace=" namespace)))))

(defn recluster!
  "Reclusters the local node."
  []
  (c/su (c/exec :asinfo :-v "recluster:")))

(defn roster
  [conn namespace]
  (->> (server-info (first (nodes conn))
                    (str "roster:namespace=" namespace))
       split-colons
       (map kv-split)
       (into {})
       (util/map-vals split-commas)))

(defn asinfo-roster-set!
  [namespace node-list]
  (c/trace
   (c/exec
     :asinfo :-v (str "roster-set:namespace=" namespace ";nodes="
                      (str/join "," node-list)))))

;;; asinfo utilities:
(defmacro poll
  "Calls `expr` repeatedly, binding the result to `sym`, and evaluating `pred`
  with `sym` bound. When predicate evaluates truthy, returns the value of sym."
  [[sym expr] & pred]
  `(loop [tries# 30]
     (when (zero? tries#)
       (throw (RuntimeException. "Timed out!")))
     (let [~sym ~expr]
       (if (do ~@pred)
         ~sym
         (do (Thread/sleep 1000)
             (recur (dec tries#)))))))

(defn wait-for-all-nodes-observed
  [conn test namespace]
  ; (info "Waiting for all nodes observed")
  (poll [result (:observed_nodes (roster conn namespace))]
        (= (count result) (count (:nodes test)))))

(defn wait-for-all-nodes-pending
  [conn test namespace]
  ; (info "Waiting for all nodes pending")
  (poll [result (:pending_roster (roster conn namespace))]
        (= (count result) (count (:nodes test)))))

(defn wait-for-all-nodes-active
  [conn test namespace]
  ; (info "Waiting for all nodes active")
  (poll [result (:roster (roster conn namespace))]
        (= (count result) (count (:nodes test)))))

(defn wait-for-migrations
  [conn]
  ; (info "Waiting for migrations")
  (poll [stats (:statistics (server-info (first (nodes conn))))]
        (and (= (:migrate_allowed stats) "true")
             (= (:migrate_partitions_remaining stats) 0))))

;;; Server setup

(defn local-packages
  "An array of canonical paths for local packages, from local-package-dir.
  Ensures that we have all required deb packages."
  []
  (let [files (->> (io/file local-package-dir)
                   (.listFiles)
                   (keep (fn [^java.io.File f]
                           (let [name (.getName f)]
                             (when (re-find #"\.deb$" (.getName f))
                               [name f]))))
                   (into (sorted-map)))]
    (assert (some (partial re-find #"aerospike-server") (keys files))
            (str "Expected an aerospike-server .deb in " local-package-dir))
    (assert (some (partial re-find #"aerospike-tools") (keys files))
            (str "Expected an aerospike-tools .deb in " local-package-dir))
    files))

(defn install!
  [node]
  (info "Installing Aerospike packages")
  (c/su
   (debian/uninstall! ["aerospike-server-*" "aerospike-tools"])
   (debian/install ["python"])
   (c/exec :mkdir :-p remote-package-dir)
   (c/exec :chmod :a+rwx remote-package-dir)
   (doseq [[name file] (local-packages)]
     (c/trace
       (c/upload (.getCanonicalPath file) remote-package-dir))
       (c/exec :dpkg :-i :--force-confnew (str remote-package-dir name)))

   ; sigh
   (c/exec :systemctl :daemon-reload)

   ; debian packages don't do this?
   (c/exec :mkdir :-p "/var/log/aerospike")
   (c/exec :chown "aerospike:aerospike" "/var/log/aerospike")
   (c/exec :mkdir :-p "/var/run/aerospike")
   (c/exec :chown "aerospike:aerospike" "/var/run/aerospike")

   ;; Replace /usr/bin/asd with a wrapper that skews time a bit
   ;(c/exec :mv "/usr/bin/asd" "/usr/local/bin/asd")
   ;(c/exec :echo
   ;        "#!/bin/bash\nfaketime -m -f \"+$((RANDOM%20))s x1.${RANDOM}\" /usr/local/bin/asd $*" :> "/usr/bin/asd")
   ;(c/exec :chmod "0755" "/usr/bin/asd")))
   ))

(defn configure!
  "Uploads configuration files to the given node."
  [node test opts]
  (info "Configuring...")
  (c/su
    ; Config file
    (c/exec :echo (-> "aerospike.conf"
                      io/resource
                      slurp
                      (str/replace "$NODE_ADDRESS" (net/local-ip))
                      (str/replace "$MESH_ADDRESS"
                                   (net/ip (jepsen/primary test)))
                      (str/replace "$REPLICATION_FACTOR"
                                   (str (:replication-factor opts)))
                      (str/replace "$HEARTBEAT_INTERVAL"
                                   (str (:heartbeat-interval opts)))
                      (str/replace "$COMMIT_TO_DEVICE"
                                   (if (:commit-to-device opts)
                                     "commit-to-device true"
                                     "")))
            :> "/etc/aerospike/aerospike.conf")))

(defn start!
  "Starts aerospike."
  [node test]
  (jepsen/synchronize test)
  (info "Starting...")
  (c/su (c/exec :service :aerospike :start))
  (info "Started")
  (jepsen/synchronize test) ;; Wait for all servers to start

  (let [conn (connect node)]
    (try
      (when (= node (jepsen/primary test))
        (asinfo-roster-set! ans (wait-for-all-nodes-observed conn test ans))
        (wait-for-all-nodes-pending conn test ans)
        (asadm-recluster!))

      (jepsen/synchronize test)
      (wait-for-all-nodes-active conn test ans)
      (wait-for-migrations conn)
      (jepsen/synchronize test)
      (finally (close conn))))

  (info node "start done"))

(defn stop!
  "Stops aerospike."
  [node]
  (info node "stopping aerospike")
  (c/su
    (meh (c/exec :service :aerospike :stop))
    (meh (c/exec :killall :-9 :asd))))

(defn wipe!
  "Shuts down the server and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
   (meh (c/exec :cp :-f (c/lit "/var/log/aerospike/aerospike.log{,.bac}")))
   (meh (c/exec :truncate :--size 0 (c/lit "/var/log/aerospike/aerospike.log")))
   (doseq [dir ["data" "smd" "udf"]]
     (c/exec :rm :-rf (c/lit (str "/opt/aerospike/" dir "/*"))))))

;;; Test:
(defn db
  [opts]
  "Aerospike for a particular version."
  (reify db/DB
    (setup! [_ test node]
      (nt/reset-time!)
      (doto node
        (install!)
        (configure! test opts)
        (start! test)))

    (teardown! [_ test node]
      (wipe! node))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/aerospike/aerospike.log"])))

(def ^Policy policy
  "General operation policy"
  (let [p (Policy.)]
    (set! (.socketTimeout p) 10000)
    (set! (.maxRetries p) 0)
    p))

(def ^Policy linearize-read-policy
   "Policy needed for linearizability testing."
   ;; FIXME - need supported client - prefer to install client from packages/.
   (let [p (Policy. policy)]
     (set! (.linearizeRead p) true)
     p))

(def ^WritePolicy write-policy
  (let [p (WritePolicy. policy)]
    p))

(defn ^WritePolicy generation-write-policy
  "Write policy for a particular generation."
  [^long g]
  (let [p (WritePolicy. write-policy)]
    (set! (.generationPolicy p) GenerationPolicy/EXPECT_GEN_EQUAL)
    (set! (.generation p) (int g))
    p))

(defn record->map
  "Converts a record to a map like

      {:generation 1
       :expiration date
       :bins {:k1 v1, :k2 v2}}"
  [^Record r]
  (when r
    {:generation (.generation r)
     :expiration (.expiration r)
     :bins       (->> (.bins r)
                      (map (fn [[k v]] [(keyword k) v]))
                      (into {}))}))

(defn map->bins
  "Takes a map of bin names (as symbols or strings) to values and emits an
  array of Bins."
  [bins]
  (->> bins
       (map (fn [[k v]] (Bin. (name k) v)))
       (into-array Bin)))

(defn put!
  "Writes a map of bin names to values to the record at the given namespace,
  set, and key."
  ([client namespace set key bins]
   (put! client write-policy namespace set key bins))
  ([^AerospikeClient client, ^WritePolicy policy, namespace set key bins]
   (.put client policy (Key. namespace set key) (map->bins bins))))

(defn put-if-absent!
  "Writes a map of bin names to bin values to the record at the given
  namespace, set, and key, if and only if the record does not presently exist."
  ([client namespace set key bins]
   (put-if-absent! client write-policy namespace set key bins))
  ([^AerospikeClient client, ^WritePolicy policy, namespace set key bins]
   (let [p (WritePolicy. policy)]
     (set! (.recordExistsAction p) RecordExistsAction/CREATE_ONLY)
     (put! client p namespace set key bins))))

(defn append!
  "Takes a namespace, set, and key, and a map of bins to values. For the record
  identified by key, appends each value in `bins` to the current value of the
  corresponding bin key."
  ([client namespace set key bins]
   (append! client write-policy namespace set key bins))
  ([^AerospikeClient client, ^WritePolicy policy, namespace set key bins]
   (.append client policy (Key. namespace set key) (map->bins bins))))


(defn fetch
  "Reads a record as a map of bin names to bin values from the given namespace,
  set, and key. Returns nil if no record found."
  [^AerospikeClient client namespace set key]
  (-> client
      (.get linearize-read-policy (Key. namespace set key))
      record->map))

(defn cas!
  "Atomically applies a function f to the current bins of a record."
  [^AerospikeClient client namespace set key f]
  (let [r (fetch client namespace set key)]
    (when (nil? r)
      (throw (ex-info "cas not found" {:namespace namespace
                                       :set set
                                       :key key})))
    (put! client
          (generation-write-policy (:generation r))
          namespace
          set
          key
          (f (:bins r)))))

(defn add!
  "Takes a client, a key, and a map of bin names to numbers, and adds those
  numbers to the corresponding bins on that record."
  [^AerospikeClient client namespace set key bins]
  (.add client write-policy (Key. namespace set key) (map->bins bins)))

(defmacro with-errors
  "Takes an invocation operation, a set of idempotent operations :f's which can
  safely be assumed to fail without altering the model state, and a body to
  evaluate. Catches errors and maps them to failure ops matching the
  invocation."
  [op idempotent-ops & body]
  `(let [error-type# (if (~idempotent-ops (:f ~op))
                       :fail
                       :info)]
     (try
       ~@body

       ; Timeouts could be either successful or failing
       (catch AerospikeException$Timeout e#
         (assoc ~op :type error-type#, :error :timeout))

       ;; Connection errors could be either successful or failing
       (catch AerospikeException$Connection e#
         (assoc ~op :type error-type#, :error :connection))

       (catch ExceptionInfo e#
         (case (.getMessage e#)
           ; Skipping a CAS can't affect the DB state
           "skipping cas"  (assoc ~op :type :fail, :error :value-mismatch)
           "cas not found" (assoc ~op :type :fail, :error :not-found)
           (throw e#)))

       (catch com.aerospike.client.AerospikeException e#
         (case (.getResultCode e#)
           ; This is error code "OK", which I guess also means "dunno"?
           0 (condp instance? (.getCause e#)
               java.io.EOFException
               (assoc ~op :type error-type#, :error :eof)

               java.net.SocketException
               (assoc ~op :type error-type#, :error :socket-error)

               (throw e#))

           ; Generation error; CAS can't have taken place.
           3 (assoc ~op :type :fail, :error :generation-mismatch)

           -8 (assoc ~op :type error-type#, :error :server-unavailable)

           ; With our custom client, these are guaranteed failures. Not so in
           ; the stock client!
           11 (assoc ~op :type :fail, :error :partition-unavailable)

           ; Hot key
           14 (assoc ~op :type :fail, :error :hot-key)

           ;; Forbidden
           22 (assoc ~op :type :fail, :error [:forbidden (.getMessage e#)])

           (do (info :error-code (.getResultCode e#))
               (throw e#)))))))
