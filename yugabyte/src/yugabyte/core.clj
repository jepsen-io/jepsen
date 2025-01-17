(ns yugabyte.core
  "Integrates workloads, nemeses, and automation to construct test maps."
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.tests :as tests]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.os.centos :as centos]
            [yugabyte [append :as append]
                      [default-value :as default-value]]
            [yugabyte.auto :as auto]
            [yugabyte.bank :as bank]
            [yugabyte.counter :as counter]
            [yugabyte.long-fork :as long-fork]
            [yugabyte.multi-key-acid :as multi-key-acid]
            [yugabyte.nemesis :as nemesis]
            [yugabyte.single-key-acid :as single-key-acid]
            [yugabyte.set :as set]
            [yugabyte.utils :refer :all]
            [yugabyte.ycql.bank]
            [yugabyte.ycql.counter]
            [yugabyte.ycql.long-fork]
            [yugabyte.ycql.multi-key-acid]
            [yugabyte.ycql.set]
            [yugabyte.ycql.single-key-acid]
            [yugabyte.ysql [append :as ysql.append]
                           [append-table :as ysql.append-table]
                           [default-value :as ysql.default-value]]
            [yugabyte.ysql.bank]
            [yugabyte.ysql.counter]
            [yugabyte.ysql.long-fork]
            [yugabyte.ysql.multi-key-acid]
            [yugabyte.ysql.set]
            [yugabyte.ysql.single-key-acid])
  (:import (jepsen.client Client)))

(defn noop-test
  "NOOP test, exists to validate setup/teardown phases"
  [opts]
  (merge tests/noop-test opts))

(defn sleep-test
  "NOOP test that gives you time to log into nodes and poke around, trying stuff manually.
  Sleeps for durations specified by --time-limit (in seconds), defaults to 60."
  [opts]
  (merge tests/noop-test
         {:client (reify Client
                    (setup! [this test]
                      (let [wait-sec (:time-limit opts)]
                        (info "Sleeping for" wait-sec "s...")
                        (Thread/sleep (* wait-sec 1000))))
                    (teardown! [this test])
                    (invoke! [this test op] (assoc op :type :ok))
                    (open! [this test node] this)
                    (close! [this test]))}
         opts))

(defn is-stub-workload
  "Whether workload defined by the given keyword is just a stub, or is a real one"
  [w]
  (or (= (name w) "none") (= (name w) "sleep")))

(defmacro with-client
  [workload client-ctor]
  "Wraps a workload function to add :client entry to the result.
  Made as macro to re-evaluate client on every invocation."
  `(fn [~'opts] (assoc (~workload ~'opts) :client ~client-ctor)))

(def workloads-ycql
  "A map of workload names to functions that can take option maps and construct workloads."
  #:ycql{:none            noop-test
         :counter         (with-client counter/workload (yugabyte.ycql.counter/->CQLCounterClient))
         :set             (with-client set/workload (yugabyte.ycql.set/->CQLSetClient))
         :set-index       (with-client set/workload (yugabyte.ycql.set/->CQLSetIndexClient))
         :bank            (with-client bank/workload-allow-neg (yugabyte.ycql.bank/->CQLBank))
         ; Shouldn't be used until we support transactions with selects.
         ; :bank-multitable (with-client bank/workload-allow-neg (yugabyte.ycql.bank/->CQLMultiBank))
         :long-fork       (with-client long-fork/workload (yugabyte.ycql.long-fork/->CQLLongForkIndexClient))
         :single-key-acid (with-client single-key-acid/workload (yugabyte.ycql.single-key-acid/->CQLSingleKey))
         :multi-key-acid  (with-client multi-key-acid/workload (yugabyte.ycql.multi-key-acid/->CQLMultiKey))})

(def workloads-ysql
  "A map of workload names to functions that can take option maps and construct workloads."
  #:ysql{:none            noop-test
         :sleep           sleep-test
         :counter         (with-client counter/workload (yugabyte.ysql.counter/->YSQLCounterClient))
         :set             (with-client set/workload (yugabyte.ysql.set/->YSQLSetClient))
         ; This one doesn't work because of https://github.com/YugaByte/yugabyte-db/issues/1554
         ; :set-index       (with-client set/workload (yugabyte.ysql.set/->YSQLSetIndexClient))
         ; We'd rather allow negatives for now because it makes reproducing error easier
         :bank            (with-client bank/workload-allow-neg (yugabyte.ysql.bank/->YSQLBankClient true))
         :bank-multitable (with-client bank/workload-allow-neg (yugabyte.ysql.bank/->YSQLMultiBankClient true))
         :long-fork       (with-client long-fork/workload (yugabyte.ysql.long-fork/->YSQLLongForkClient))
         :single-key-acid (with-client single-key-acid/workload (yugabyte.ysql.single-key-acid/->YSQLSingleKeyAcidClient))
         :multi-key-acid  (with-client multi-key-acid/workload (yugabyte.ysql.multi-key-acid/->YSQLMultiKeyAcidClient))
         :append          (with-client append/workload (ysql.append/->Client))
         :append-table    (with-client append/workload (ysql.append-table/->Client))
         :default-value   (with-client default-value/workload (ysql.default-value/->Client))})

(def workloads
  (merge workloads-ycql workloads-ysql))

(def workload-options
  "For each workload, a map of workload options to all the values that option
  supports. Used for test-all."
  ; If we ever need additional options - merge them onto this base set
  (merge (map-values workloads-ycql (fn [_] {}))
         (map-values workloads-ysql (fn [_] {}))))

(def workload-options-expected-to-pass
  "Only workloads and options that we think should pass. Also used for
  test-all."
  (-> workload-options
      (dissoc :ycql/bank-multitable
              :ycql/none
              :ysql/none
              :ysql/sleep
              :ysql/append-table)))

(def nemesis-specs
  "These are the types of failures that the nemesis can perform."
  #{:partition
    :partition-half
    :partition-ring
    :partition-one
    :kill
    :kill-master
    :kill-tserver
    :pause-master
    :pause-tserver
    :pause
    :stop
    :stop-master
    :stop-tserver
    :clock-skew})

(def all-nemeses
  "All nemesis specs to run as a part of a complete test suite."
  (->> [[]                                                  ; No faults
        [:kill-tserver]                                     ; Just tserver
        [:kill-master]                                      ; Just master
        [:pause-tserver]                                    ; Just pause tserver
        [:pause-master]                                     ; Just pause master
        [:clock-skew]                                       ; Just clocks
        [:partition-one                                     ; Just partitions
         :partition-half
         :partition-ring]
        [:kill-tserver
         :kill-master
         :pause-tserver
         :pause-master
         :clock-skew
         :partition-one
         :partition-half
         :partition-ring]]
       ; Turn these into maps with each key being true
       (map (fn [faults] (zipmap faults (repeat true))))))

(defn yugabyte-ssh-defaults
  "A partial test map with SSH options for a test running in Yugabyte's
  internal testing environment."
  []
  {:ssh {:port                     54422
         :strict-host-key-checking false
         :username                 "yugabyte"
         :private-key-path         (str (System/getenv "HOME")
                                        "/.yugabyte/yugabyte-dev-aws-keypair.pem")}})

(def trace-logging
  "Logging configuration for the test which sets up traces for queries."
  {:logging {:overrides {;"com.datastax"                            :trace
                         ;"com.yugabyte"                            :trace
                         "com.datastax.driver.core.RequestHandler" :trace
                         ;"com.datastax.driver.core.CodecRegistry"  :info
                         }}})

(defn all-combos
  "Takes a map of options to collections of values for that option. Computes a
  collection of maps with the combinatorial expansion of every possible option
  value."
  ([opts]
   (all-combos {} opts))
  ([m opts]
   (if (seq opts)
     (let [[k vs] (first opts)]
       (mapcat (fn [v]
                 (all-combos (assoc m k v) (next opts)))
               vs))
     (list m))))

(defn all-workload-options
  "Expands workload-options into all possible CLI opts for each combination of
  workload options."
  [workload-options]
  (mapcat (fn [[workload opts]]
            (all-combos {:workload workload} opts))
          workload-options))

(defn test-1
  "Initial test construction from a map of CLI options. Establishes the test
  name, OS, DB."
  [opts]
  (assoc opts
    :name (str "yb " (-> (or (:url opts) (:version opts))
                         (str/split #"/")
                         (last))
               " " (name (:workload opts))
               (when-not (= [:interval] (keys (:nemesis opts)))
                 (str " nemesis " (->> (dissoc (:nemesis opts) :interval)
                                       keys
                                       (map name)
                                       sort
                                       (str/join ",")))))
    :os (case (:os opts)
          :centos centos/os
          :debian debian/os)
    :db (auto/->YugaByteDB)))

(defn test-2
  "Second phase of test construction. Builds the workload and nemesis, and
  finalizes the test."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        nemesis  (nemesis/nemesis opts)
        api      (keyword (namespace (:workload opts)))
        gen      (->> (:generator workload)
                      (gen/nemesis (:generator nemesis))
                      (gen/time-limit (:time-limit opts)))
        gen      (if (:final-generator workload)
                   (gen/phases gen
                               (gen/log "Healing cluster")
                               (gen/nemesis (:final-generator nemesis))
                               (gen/log "Waiting for recovery...")
                               (gen/sleep (:final-recovery-time opts))
                               (gen/clients (:final-generator workload)))
                   gen)
        perf     (checker/perf
                   {:nemeses #{{:name       "kill master"
                                :start      #{:kill-master :stop-master}
                                :stop       #{:start-master}
                                :fill-color "#E9A4A0"}
                               {:name       "kill tserver"
                                :start      #{:kill-tserver :stop-tserver}
                                :stop       #{:start-tserver}
                                :fill-color "#E9C3A0"}
                               {:name       "pause master"
                                :start      #{:pause-master}
                                :stop       #{:resume-master}
                                :fill-color "#A0B1E9"}
                               {:name       "pause tserver"
                                :start      #{:pause-tserver}
                                :stop       #{:resume-tserver}
                                :fill-color "#B8A0E9"}
                               {:name       "clock skew"
                                :start      #{:bump-clock :strobe-clock}
                                :stop       #{:reset-clock}
                                :fill-color "#D2E9A0"}
                               {:name       "partition"
                                :start      #{:start-partition}
                                :stop       #{:stop-partition}
                                :fill-color "#888888"}}})
        checker  (if (is-stub-workload (:workload opts))
                   (:checker workload)
                   (checker/compose {:perf     perf
                                     :stats    (checker/stats)
                                     :unhandled-exceptions (checker/unhandled-exceptions)
                                     :clock    (checker/clock-plot)
                                     :workload (:checker workload)}))]
    (merge tests/noop-test
           opts
           (dissoc workload
                   :generator
                   :final-generator
                   :checker)
           (when (:yugabyte-ssh opts) (yugabyte-ssh-defaults))
           (when (:trace-cql opts) (trace-logging))
           {:api       api
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator gen
            :checker   checker})))

(defn yb-test
  "Constructs a yugabyte test from CLI options."
  [opts]
  (-> opts test-1 test-2))
