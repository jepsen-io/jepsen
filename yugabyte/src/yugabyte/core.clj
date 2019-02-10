(ns yugabyte.core
  "Integrates workloads, nemeses, and automation to construct test maps."
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os [debian :as debian]
                       [centos :as centos]]
            [yugabyte [auto :as auto]
                      [bank :as bank]
                      [counter :as counter]
                      [long-fork :as long-fork]
                      [multi-key-acid :as multi-key-acid]
                      [nemesis :as nemesis]
                      [single-key-acid :as single-key-acid]
                      [set :as set]]))

(def workloads
  "A map of workload names to functions that can take option maps and construct
  workloads."
  {:bank              bank/workload
   :bank-multitable   bank/multitable-workload
   :counter           counter/workload
   :counter-dec       counter/workload-dec
   :long-fork         long-fork/workload
   :multi-key-acid    multi-key-acid/workload
   :set               set/workload
   :set-index         set/index-workload
   :single-key-acid   single-key-acid/workload})

(def workload-options
  "For each workload, a map of workload options to all the values that option
  supports."
  {})

(defn yugabyte-ssh-defaults
  "A partial test map with SSH options for a test running in Yugabyte's
  internal testing environment."
  []
  {:ssh {:port                     54422
         :strict-host-key-checking false
         :username                 "yugabyte"
         :private-key-path (str (System/getenv "HOME")
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
         :name (str "yb " (:version opts)
                    " " (name (:workload opts)))
;                    (when-not (= [:interval] (keys (:nemesis opts)))
;                      (str " nemesis " (->> (dissoc (:nemesis opts) :interval)
;                                            keys
;                                            (map name)
;                                            sort
;                                            (str/join ",")))))
         :os (case (:os opts)
               :centos centos/os
               :debian debian/os)
         :db (case (:db opts)
               :community-edition   (auto/community-edition)
               :enterprise-edition  (auto/enterprise-edition))))

(defn test-2
  "Second phase of test construction. Builds the workload and nemesis, and
  finalizes the test."
  [opts]
  (let [workload  ((get workloads (:workload opts)) opts)
        gen       (->> (:generator workload)
                       (gen/nemesis (nemesis/gen opts))
                       (gen/time-limit (:time-limit opts)))
        gen       (if (:final-generator workload)
                    (gen/phases gen
                                (gen/log "Healing cluster")
                                ;(gen/nemesis (:final-generator nemesis))
                                (gen/nemesis (nemesis/final-gen opts))
                                (gen/log "Waiting for recovery...")
                                (gen/sleep (:final-recovery-time opts))
                                (gen/clients (:final-generator workload)))
                    gen)]
    (merge opts
           (dissoc workload
                   :generator
                   :final-generator
                   :checker)
           (when (:yugabyte-ssh opts) (yugabyte-ssh-defaults))
           (when (:trace-cql opts)    (trace-logging))
           {:client     (:client workload)
            :nemesis    (nemesis/get-nemesis-by-name (:nemesis opts))
            :generator  gen
            :checker    (checker/compose {:perf (checker/perf)
                                          :clock (checker/clock-plot)
                                          :workload (:checker workload)})
            :max-clock-skew-ms  (nemesis/get-nemesis-max-clock-skew-ms opts)})))

(defn yb-test
  "Constructs a yugabyte test from CLI options."
  [opts]
  (-> opts test-1 test-2))
