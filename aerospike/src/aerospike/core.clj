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

(defn nodes
  "Nodes known to a client."
  [^AerospikeClient client]
  (vec (.getNodes client)))

(defn split-semicolons
  "Splits a semicolon-delimited string."
  [s]
  (str/split s #";"))

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
                                      (map (fn [pair]
                                             (let [[k v] (str/split pair #"=")]
                                               [(keyword k)
                                                (util/maybe-number v)])))
                                      (into (sorted-map)))
                     (util/maybe-number v))])))
       (into (sorted-map))))

(defn close
  [^java.io.Closeable x]
  (.close x))

(defn connect
  "Returns a client for the given node. Blocks until the client is available."
  [node]
  (let [client (AerospikeClient. (name node) 3000)]
    ; Wait for connection
    (while (not (.isConnected client))
      (Thread/sleep 10))
    ; Wait for workable ops
    (while (try (server-info (first (nodes client)))
                false
                (catch AerospikeException$Timeout e
                  true))
      (Thread/sleep 10))
    ; Wait more
    client))

(defn all-node-ids
  "Given a test, finds the sorted list of node IDs across the cluster."
  [test]
  (->> test
       :nodes
       (pmap (fn [node]
               (with-open [c (connect node)]
                 (vec (.getNodeNames c)))))
       flatten
       sort))

(defn install!
  "Installs aerospike on the given node."
  [node version]
  (when-not (= (str version "-1")
               (debian/installed-version "aerospike-server-community"))
               ; lol they don't package the same version of the tools
               ; (debian/installed-version "aerospike-tools"))
    (debian/install ["python"])
    (c/su
      (debian/uninstall! ["aerospike-server-community" "aerospike-tools"])
      (info node "installing aerospike" version)
      (c/cd "/tmp"
            (c/exec :wget :-O "aerospike.tgz"
                    (str "http://www.aerospike.com/download/server/" version
                         "/artifact/debian7"))
            (c/exec :tar :xvfz "aerospike.tgz"))
      (c/cd (str "/tmp/aerospike-server-community-" version "-debian7")
            (c/exec :dpkg :-i (c/lit "aerospike-server-community-*.deb"))
            (c/exec :dpkg :-i (c/lit "aerospike-tools-*.deb")))

      ; Replace /usr/bin/asd with a wrapper that skews time a bit
      (c/exec :mv   "/usr/bin/asd" "/usr/local/bin/asd")
      (c/exec :echo
              "#!/bin/bash\nfaketime -m -f \"+$((RANDOM%100))s x1.${RANDOM}\" /usr/local/bin/asd" :> "/usr/bin/asd")
      (c/exec :chmod "0755" "/usr/bin/asd"))))

(defn configure!
  "Uploads configuration files to the given node."
  [node test]
  (c/su
    ; Config file
    (c/exec :echo (-> "aerospike.conf"
                      io/resource
                      slurp
                      (str/replace "$NODE_ADDRESS" (net/local-ip))
                      (str/replace "$MESH_ADDRESS"
                                   (net/ip (jepsen/primary test))))
            :> "/etc/aerospike/aerospike.conf")))

(defn dun!
  "DUN DUN DUUUNNNNNN"
  [nodes]
  (c/su (c/exec :asinfo :-v (str "dun:nodes=" (str/join "," nodes)))))

(defn start!
  "Starts aerospike."
  [node test]
  (info node "starting aerospike")
  (c/su
    (c/exec :service :aerospike :start)

    ; Enable auto-dunning as per
    ; http://www.aerospike.com/docs/operations/troubleshoot/cluster/
    ; This doesn't seem to actually do anything but ???
    (c/exec :asinfo :-v
      "config-set:context=service;paxos-recovery-policy=auto-dun-master")))

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
    (doseq [dir ["data" "smd" "udf"]]
      (c/exec :truncate :--size 0 "/var/log/aerospike/aerospike.log")
      (c/exec :rm :-rf (c/lit (str "/opt/aerospike/" dir "/*"))))))

(defn db [version]
  "Aerospike for a particular version."
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)
        (configure! test)
        (start! test)))

    (teardown! [_ test node]
      (wipe! node))))


(def ^Policy policy
  "General operation policy"
  (let [p (Policy.)]
    (set! (.timeout p) 100000)
    ; (set! (.consistencyLevel p) ConsistencyLevel/CONSISTENCY_ALL)
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
  ([^AerospikeClient client ^WritePolicy policy, namespace set key bins]
   (.put client policy (Key. namespace set key) (map->bins bins))))

(defn fetch
  "Reads a record as a map of bin names to bin values from the given namespace,
  set, and key. Returns nil if no record found."
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
           (throw e#))))))

(defrecord CasRegisterClient [client namespace set key]
  client/Client
  (setup! [this test node]
    (let [client (connect node)]
      (Thread/sleep 10000)
      (put! client namespace set key {:value nil})
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
          :os      debian/os
          :db      (db "3.5.4")
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
