(ns aerospike.support
  "Core DB setup and support features"
  (:require
   [clojure [string :as str]]
   [clojure.java.io :as io]
   [clojure.tools.logging :refer [debug info warn]]
   [dom-top.core :refer [with-retry]]
   [jepsen 
    [core      :as jepsen]
    [db        :as db]
    [util      :as util :refer [meh timeout]]
    [control   :as c :refer [|]]]
   [jepsen.nemesis.time :as nt]
   [jepsen.os.debian :as debian])

  (:import
  ;;  (asx Asx
  ;;       AsxClient)
   (clojure.lang ExceptionInfo)
   (com.aerospike.client IAerospikeClient
                         AerospikeClient
                         AerospikeException
                         AerospikeException$Timeout
                         Bin
                         Info
                         Operation
                         Key
                         Record
                         Value)
   (com.aerospike.client.cdt ListOperation
                             ListPolicy)
   (com.aerospike.client.cluster Node)
   (com.aerospike.client.policy Policy
                                ClientPolicy
                                RecordExistsAction
                                ReadModeSC
                                GenerationPolicy
                                WritePolicy)))

(def local-package-dir
  "Local directory for Aerospike packages."
  "packages/")

(def remote-package-dir
  "/tmp/packages/")

(def ans "Aerospike namespace"
  "test")

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
  [^IAerospikeClient client]
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
  (.close x)
  ;; (.terminateServer x)
  )


(defn fSingleNode-policy
  []
  (let [cp (ClientPolicy.)]
    (set! (.forceSingleNode cp) true)
    cp))

(defn create-client
  "Spinloop to create a client, since our hacked version will crash on init if
  it can't connect."
  [node]
  ;; (info "CREATING CLIENT")
  (with-retry [tries 30]
    (AerospikeClient. (fSingleNode-policy) node 3000)
    (catch com.aerospike.client.AerospikeException e
      (when (zero? tries)
        (throw e))
      (info "Retrying client creation -" (.getMessage e))
      (Thread/sleep 1000)
      ('retry (dec tries)))))


(defn connect
  "Returns a client for the given node. Blocks until the client is available."
  [node]
  ;; FIXME - client no longer blocks till cluster is ready.
  (let [client (create-client node)]
    ; Wait for connection
    (while (not (.isConnected ^IAerospikeClient client))
      (Thread/sleep 100))

    ;; (info "CLIENT CONNECTED!")
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
  (c/trace (c/su (c/exec :asadm :-e "enable; asinfo -v recluster:"))))

(defn revive!
  "Revives a namespace on the local node."
  ([]
   (revive! ans))
  ([namespace]
   (c/trace 
    (c/su (c/exec :asinfo :-v (str "revive:namespace=" namespace)))
   )
  ))

(defn recluster!
  "Reclusters the local node."
  []
  (c/trace
    (c/su (c/exec :asinfo :-v "recluster:"))
  )
  )

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
(defn allow-expunge!
  [namespace]
  ;; (info "Setting ns-config item `strong-consistency-allow-expunge` to TRUE")
  (c/trace 
   (c/exec :asinfo :-v 
           (str "set-config:context=namespace;id=" namespace ";strong-consistency-allow-expunge=true"))))

(defn clear-data!
  [namespace]
  (c/trace (c/su (c/exec 
        :asinfo :-v (str "truncate-namespace:namespace=" namespace)))))


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
  (info "Waiting for all nodes observed")
  (poll [result (:observed_nodes (roster conn namespace))]
        (= (count result) (count (:nodes test)))))

(defn wait-for-all-nodes-pending
  [conn test namespace]
  (info "Waiting for all nodes pending")
  (poll [result (:pending_roster (roster conn namespace))]
        (= (count result) (count (:nodes test)))))

(defn wait-for-all-nodes-active
  [conn test namespace]
  (info "Waiting for all nodes active")
  (poll [result (:roster (roster conn namespace))]
        (= (count result) (count (:nodes test)))))

(defn wait-for-migrations
  [conn]
  (info "Waiting for migrations")
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
    ;; Bob tests already have server installed
    ;;(assert (some (partial re-find #"aerospike-server") (keys files))
    ;;        (str "Expected an aerospike-server .deb in " local-package-dir))
    (assert (some (partial re-find #"aerospike-tools") (keys files))
            (str "Expected an aerospike-tools .deb in " local-package-dir))
    files))

(defn install!
  [node]
  (info "Installing Aerospike packages")
  (c/su
   ;; Bob tests should not need to clean up before test run
   ;;(debian/uninstall! ["aerospike-server-*" "aerospike-tools"])
   (debian/install ["python-is-python3"])
  ;;  (debian/install ["python"])
   (c/exec :mkdir :-p remote-package-dir)
   (c/exec :chmod :a+rwx remote-package-dir)
   (doseq [[name file] (local-packages)]
       (c/trace
    ;;   ;;  (c/upload (.getPath (io/resource "features.conf")) "/etc/aerospike/"))
       (c/upload (.getCanonicalPath file) (str remote-package-dir name)))
       (info "Uploaded" (.getCanonicalPath file) " to " (str remote-package-dir name))
    ;;    (info "--AND" (.getPath (io/resource "features.conf")) " to /etc/aerospike/features.conf")       

      ; handle "dpkg was interrupted, you must manually run 
      ; 'sudo dpkg --configure -a' to correct the problem."
       (c/exec :dpkg :--configure :-a)    
      ; do package install
       (c/exec :dpkg :-i :--force-confnew (str remote-package-dir name)))
   ;; Bob should already handle updating features.conf
   ;;(c/upload (.getPath (io/resource "features.conf")) "/etc/aerospike/")
   ; sigh
   ;; systemctl not applicable for docker
   ;;(c/exec :systemctl :daemon-reload)

   ; debian packages don't do this?
   (c/exec :mkdir :-p "/var/log/aerospike")
   (c/exec :chown "aerospike:aerospike" "/var/log/aerospike")
   (c/exec :mkdir :-p "/var/run/aerospike")
   (c/exec :chown "aerospike:aerospike" "/var/run/aerospike")
   (info "DONE with install & config!")
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
;; Bob owns the initial static config file - but diff may result in expected failures
  (c/su
    ; Copy test data's Config & Feature-Key files
      (c/exec :cp "/data/aerospike_a/etc/aerospike/aerospike.conf" "/etc/aerospike/aerospike.conf")
      (c/exec :cp "/data/aerospike_a/etc/aerospike/features.conf" "/etc/aerospike/features.conf")
   )
  )

(defn start!
  "Starts aerospike."
  [node test]
  (info "Syncronizing...")
  (jepsen/synchronize test)
  (info "Syncronized!")
  (info "Starting...")
  ;; Use bob image wrapper with workaround for jepsen spawned process issue
  (c/exec "bash" "-c" "ulimit -n 15000 && aerospikectl start asd || echo 'started with ulimit'")
  (info "Started")
  (jepsen/synchronize test) ;; Wait for all servers to start

  (let [conn (connect node)]
    (try
      (when (= node (jepsen/primary test))
        (info "Setting roster using observed nodes " (:observed_nodes (roster conn ans)))
        (info "Expected to match count of " (:nodes test) " ==> " (count (:nodes test)))
        (asinfo-roster-set! ans (wait-for-all-nodes-observed conn test ans))
        (allow-expunge! ans)
        (wait-for-all-nodes-pending conn test ans)
        (asadm-recluster!)
        (clear-data! "test"))

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
    ;; use bob asd process wrapper
    (meh (c/exec :aerospikectl :stop :asd))
    (meh (c/exec :killall :-9 :asd))))

(defn wipe!
  "Shuts down the server and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
   ;; bob runs shouldn't need to wipe logs
   ;;(meh (c/exec :cp :-f (c/lit "/var/log/aerospike/aerospike.log{,.bac}")))
   ;;(meh (c/exec :truncate :--size 0 (c/lit "/var/log/aerospike/aerospike.log")))
   (doseq [dir ["data" "smd" "udf"]]
     (c/exec :rm :-rf (c/lit (str "/opt/aerospike/" dir "/*"))))))

;;; Test:
(defn db
  "Aerospike for a particular version."
  [opts]
  (info "CREATING DB")
  ;; (Asx/disableDebugMode)
  ;; (Asx/enableStaticPortMode)
  (reify db/DB
    (setup! [_ test node]
      (info "Resetting clocktimes!")
      (nt/reset-time!)
      (info "Running Install & Configuration!")
      (doto node
        (install!)
        ;; Bob owns the initial static config file - but diff may result in expected failures
        (configure! test opts)
        (start! test))
      (info "Done w/ db.setup! call."))

    ;; Bob will teardown
    (teardown! [_ test node]
               (info "IN db/teardown! CALL!") 
              ;; (clear-data! "test")
              ;; (debian/uninstall! ["aerospike-server-*" "aerospike-tools"])
               )
      ;;(wipe! node))

    )
    ;;db/LogFiles
    ;;(log-files [_ test node]
    ;;  ["/var/log/aerospike/aerospike.log"]))
  ;; (info "DONE!!  (DB->)" (db opts))
  )

(def ^Policy policy
  "General operation policy"
  (let [p (Policy.)]
    (set! (.socketTimeout p) 30000)
    (set! (.totalTimeout p) 5000)
    (set! (.maxRetries p) 0)
    p))

(defn ^Policy linearize-read-policy 
  "Policy needed for linearizability testing."
  []
  (let [p (Policy. policy)]
      ;; needed reads to retry for set workload
    (set! (.maxRetries p) 2)
    (set! (.sleepBetweenRetries p) 300)
    (set! (.readModeSC p) ReadModeSC/LINEARIZE)
    p))

(defn ^WritePolicy write-policy
  []
  (let [p (WritePolicy. policy)]
    p))

(defn ^WritePolicy generation-write-policy
  "Write policy for a particular generation."
  [^long g]
  (let [p (WritePolicy. (write-policy))]
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
   (put! client (write-policy) namespace set key bins))
  ([^IAerospikeClient client, ^WritePolicy policy, namespace set key bins]
   (info "CALLING PUT() w/ key: (" namespace ", " set ", " key   ") -- bins: " bins)
   (.put client policy (Key. namespace set key) (map->bins bins))))

(defn put-if-absent!
  "Writes a map of bin names to bin values to the record at the given
  namespace, set, and key, if and only if the record does not presently exist."
  ([client namespace set key bins]
   (put-if-absent! client (write-policy) namespace set key bins))
  ([^IAerospikeClient client, ^WritePolicy policy, namespace set key bins]
   (let [p (WritePolicy. policy)]
     (set! (.recordExistsAction p) RecordExistsAction/CREATE_ONLY)
     (put! client p namespace set key bins))))

(defn append!
  "Takes a namespace, set, and key, and a map of bins to values. For the record
  identified by key, appends each value in `bins` to the current value of the
  corresponding bin key."
  ([client namespace set key bins]
   (append! client (write-policy) namespace set key bins))
  ([^IAerospikeClient client, ^WritePolicy policy, namespace set key bins]
   (.append client policy (Key. namespace set key) (map->bins bins))))

(def sendKey-WritePolicy
  (let [p (write-policy)]
    (set! (.sendKey p) true)
    p))

(defn list-append!  [^IAerospikeClient client, ^WritePolicy policy, namespace set key bins]
  (let [pk (Key. namespace set key)
        binName (name :value)
        binVal (:value bins)
        op (ListOperation/append 
            ^ListPolicy (ListPolicy.) 
            ^String binName 
            ^Value (Value/get binVal) nil)]
    (doto (.operate ^IAerospikeClient client
                    ^WritePolicy policy
                    ^Key pk
                    (into-array [^Operation op])))))

(defn fetch
  "Reads a record as a map of bin names to bin values from the given namespace,
  set, and key. Returns nil if no record found."
  ([^IAerospikeClient client namespace set key]
   (-> client
       (.get (linearize-read-policy) (Key. namespace set key))
       record->map))
  ([^IAerospikeClient client namespace set key trid]
   (let [p (linearize-read-policy)]
     (set! (.txn p) trid)
     (info "CALLING GET() w/ key: (" namespace ", " set ", " key   ")" )
     (-> client
         (.get p (Key. namespace set key))
         record->map))))


(defn cas!
  "Atomically applies a function f to the current bins of a record."
  [^IAerospikeClient client namespace set key f]
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
  [^IAerospikeClient client namespace set key bins]
  (.add client (write-policy) (Key. namespace set key) (map->bins bins)))

(defmacro with-modern-errors
  "Takes an invocation operation as `with-errors`, but uses Exception's Indoubt
   instead of relying on idempotent ops to determine more strictly whether an
   attempt definitely failed"
  [op & body]
  `(try ~@body
       (catch AerospikeException e#
         (if (.getInDoubt e#)
           (assoc ~op :type :info, :error (.getMessage e#))
           (assoc ~op :type :fail, :error (.getMessage e#))))
       (catch ExceptionInfo e#
         (case (.getMessage e#)
                  ; Skipping a CAS can't affect the DB state
           "skipping cas"  (assoc ~op :type :fail, :error :value-mismatch)
           "cas not found" (assoc ~op :type :fail, :error :not-found)))))