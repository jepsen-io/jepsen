(ns aerospike.core
  (:require [clojure [pprint :refer :all]
             [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [os        :as os]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.debian :as debian]
            [knossos.core :as knossos])
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
                                        WritePolicy)))

(def local-package-dir
  "Local directory for Aerospike packages."
  "packages/")

(def remote-package-dir
  "/tmp/packages/")

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

;;; asinfo commands:
(defn asadm-recluster!
  []
  ;; Sends to all clustered nodes.
  (c/trace (c/su (c/exec :asadm :-e "asinfo -v recluster:"))))

(defn asinfo-roster
  [namespace]
  (let [roster-raw (c/trace
                    (c/exec :asinfo :-v (str "roster:namespace=" namespace)))
        roster-list (split-colons roster-raw)
        roster-kv-list (map kv-split roster-list)
        roster-map (into {} roster-kv-list)]
    roster-map))

(defn asinfo-roster-set!
  [namespace node-list]
  (c/trace
   (c/exec
    :asinfo :-v (str "roster-set:namespace=" namespace ";nodes=" node-list))))

(defn asinfo-statistics
  []
  (let [stats-raw (c/trace (c/exec :asinfo :-v (str "statistics")))
        stats-list (split-semicolons stats-raw)
        stats-kv-list (map kv-split stats-list)]
    (into {} stats-kv-list)))

;;; asinfo utilities:
(defn poll
  [poll-fn, check-fn]
  (loop [tries 30]
    (when (zero? tries)
      (throw (RuntimeException. "Timed out!")))
    (let [result (poll-fn)]
      (if (check-fn result)
        result
        (do (Thread/sleep 1000)
            (recur (dec tries)))))))

(defn wait-for-all-nodes-observed
  [namespace]
  (info "Waiting for all nodes observed")
  (poll (fn [] (:observed_nodes (asinfo-roster namespace)))
        (fn [result]
          (info result)
          (= (str-list-count result) 5))))

(defn wait-for-all-nodes-pending
  [namespace]
  (info "Waiting for all nodes pending")
  (poll (fn [] (:pending_roster (asinfo-roster namespace)))
        (fn [result]
          (info result)
          (= (str-list-count result) 5))))

(defn wait-for-all-nodes-active
  [namespace]
  (info "Waiting for all nodes active")
  (poll (fn [] (:roster (asinfo-roster namespace)))
        (fn [result]
          (info result)
          (= (str-list-count result) 5))))

(defn wait-for-migrations
  []
  (info "Waiting for migrations")
  (poll (fn [] (asinfo-statistics))
        (fn [stats] (and (= (:migrate_allowed stats) "true")
                         (= (:migrate_partitions_remaining stats) 0)))))

;;; Client functions:
(defn nodes
  "Nodes known to a client."
  [^AerospikeClient client]
  (vec (.getNodes client)))

(defn server-info
  "Requests server info from a particular client node."
  [^Node node]
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
       (into (sorted-map))))

(defn close
  [^java.io.Closeable x]
  (.close x))

(defn connect
  "Returns a client for the given node. Blocks until the client is available."
  [node]
  (info node "Client is attempting to connect")

  ;; FIXME - client no longer blocks till cluster is ready.

  (let [client (AerospikeClient. (name node) 3000)]
    ; Wait for connection
    (while (not (.isConnected client))
      (Thread/sleep 100))
    ; Wait for workable ops
    (while (try (server-info (first (nodes client)))
                false
                (catch AerospikeException$Timeout e
                  true))
      (Thread/sleep 10))
    ; Wait more
    client))

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
  (info node "Installing Aerospike packages")
  (c/su
   (debian/uninstall! ["aerospike-server-*" "aerospike-tools"])
   (debian/install ["python" "faketime" "psmisc"])
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
   (c/exec :mv "/usr/bin/asd" "/usr/local/bin/asd")
   (c/exec :echo
           "#!/bin/bash\nfaketime -m -f \"+$((RANDOM%20))s x1.${RANDOM}\" /usr/local/bin/asd $*" :> "/usr/bin/asd")
   (c/exec :chmod "0755" "/usr/bin/asd")))

(defn configure!
  "Uploads configuration files to the given node."
  [node test]
  (info node "Configuring...")
  (c/su
    ; Config file
    (c/exec :echo (-> "aerospike.conf"
                      io/resource
                      slurp
                      (str/replace "$NODE_ADDRESS" (net/local-ip))
                      (str/replace "$MESH_ADDRESS"
                                   (net/ip (jepsen/primary test))))
            :> "/etc/aerospike/aerospike.conf")))

(defn start!
  "Starts aerospike."
  [node test]
  (jepsen/synchronize test)
  (info node "Starting...")
  (c/su (c/exec :service :aerospike :start))
  (info node "Started")
  (jepsen/synchronize test) ;; Wait for all servers to start

  (when (= node (jepsen/primary test))
    (asinfo-roster-set! "jepsen" (wait-for-all-nodes-observed "jepsen"))
    (wait-for-all-nodes-pending "jepsen")
    (asadm-recluster!))

  (jepsen/synchronize test)
  (wait-for-all-nodes-active "jepsen")
  (wait-for-migrations)
  (jepsen/synchronize test)

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
  []
  "Aerospike for a particular version."
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install!)
        (configure! test)
        (start! test)
        ))

    (teardown! [_ test node]
      (wipe! node))))

(def ^Policy policy
  "General operation policy"
  (let [p (Policy.)]
    (set! (.socketTimeout p) 100000)
    p))

;; (def ^Policy linearize-read-policy
;;   "Policy needed for linearizability testing."
;;   ;; FIXME - need supported client - prefer to install client from packages/.
;;   (let [p (Policy. policy)]
;;     (set! (.linearizeReads p) true)
;;     p))

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
  ([^AerospikeClient client ^WritePolicy policy, namespace set key bins]
   (.put client policy (Key. namespace set key) (map->bins bins))))

(defn fetch
  "Reads a record as a map of bin names to bin values from the given namespace,
  set, and key. Returns nil if no record found."
  ;; FIXME - use linearize-read-policy.
  [^AerospikeClient client namespace set key]
  (-> client
      (.get policy (Key. namespace set key))
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
         (assoc ~op :type error-type#, :error :timeout))

       (catch ExceptionInfo e#
         (case (.getMessage e#)
           ; Skipping a CAS can't affect the DB state
           "skipping cas"  (assoc ~op :type :fail, :error :value-mismatch)
           "cas not found" (assoc ~op :type :fail, :error :not-found)
           (throw e#)))

       (catch com.aerospike.client.AerospikeException e#
         (case (.getResultCode e#)
           ; Generation error; CAS can't have taken place.
           3 (assoc ~op :type :fail, :error :generation-mismatch)
           ;; Unavailable error: CAS can't have taken place.
           11 (assoc ~op :type :fail, :error :unavailable)
           22 (assoc ~op :type :fail, :error :forbidden)
           (throw e#))))))

(defrecord CasRegisterClient [client namespace set key]
  client/Client
  (setup! [this test node]
    (let [client (connect node)]
      (Thread/sleep 10000)
      (assoc this :client client)))

  (invoke! [this test op]
    (with-errors op #{:read}
      (case (:f op)
        :read (assoc op
                     :type :ok,
                     :value (-> client (fetch namespace set key) :bins :value))

        :cas   (let [[v v'] (:value op)]
                 (cas! client namespace set key
                       (fn [r]
                         ; Verify that the current value is what we're cas'ing
                         ; from
                         (when (not= v (:value r))
                           (throw (ex-info "skipping cas" {})))
                         {:value v'}))
                 (assoc op :type :ok))

        :write (do (put! client namespace set key {:value (:value op)})
                   (assoc op :type :ok)))))

  (teardown! [this test]
    (close client)))

(defn cas-register-client
  "A basic CAS register on top of a single key and bin."
  []
  (CasRegisterClient. nil "jepsen" "cats" "mew"))

(defrecord CounterClient [client namespace set key]
  client/Client
  (setup! [this test node]
    (let [client (connect node)]
      (Thread/sleep 3000)
      (put! client namespace set key {:value 0})
      (assoc this :client client)))

  (invoke! [this test op]
    (with-errors op #{:read}
      (case (:f op)
        :read (assoc op :type :ok
                     :value (-> client (fetch namespace set key) :bins :value))

        :add  (do (add! client namespace set key {:value (:value op)})
                  (assoc op :type :ok)))))

  (teardown! [this test]
    (close client)))

(defn counter-client
  "A basic counter."
  []
  (CounterClient. nil "jepsen" "counters" "pounce"))

; Nemeses

(defn killer
  "Kills aerospike on a random node on start, restarts it on stop."
  []
  (nemesis/node-start-stopper
    rand-nth
    (fn start [test node] (c/su (c/exec :killall :-9 :asd)))
    (fn stop  [test node] (start! node test))))

; Generators

(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn add [_ _] {:type :invoke, :f :add, :value 1})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  [gen]
  (gen/phases
    (->> gen
         (gen/nemesis
           (gen/seq (cycle [(gen/sleep 2)
                            {:type :info :f :start}
                            (gen/sleep 10)
                            {:type :info :f :stop}])))
         (gen/time-limit 60))
    ; Recover
    (gen/nemesis
      (gen/once {:type :info :f :stop}))
    ; Wait for resumption of normal ops
    (gen/clients
      (->> gen
           (gen/time-limit 5)))))

(defn aerospike-test
  [name opts]
  (merge tests/noop-test
         {:name    (str "aerospike " name)
          :os      os/noop
          :db      (db)
          :ssh     {:username "admin"
                    :password ""}
          :nodes   (-> (slurp "/home/admin/nodes")
                       (str/split #"\n"))
          :model   (model/cas-register)
          :checker (checker/compose {:linear checker/linearizable
                                     :perf (checker/perf)})
          :nemesis (nemesis/partition-random-halves)}
         opts))

(defn cas-register-test
  []
  (aerospike-test "cas register"
                  {:client    (cas-register-client)
;                   :nemesis   (nemesis/hammer-time "asd")
;                   :nemesis   (nemesis/noop)
                   :generator (->> [r w cas cas cas]
                                   gen/mix
                                   (gen/delay 1)
                                   std-gen)}))

(defn counter-test
  []
  (aerospike-test "counter"
                  {:client    (counter-client)
                   :generator (->> (repeat 100 add)
                                   (cons r)
                                   gen/mix
                                   (gen/delay 1/100)
                                   std-gen)
                   :checker   (checker/compose {:counter checker/counter
                                                :perf    (checker/perf)})}))
