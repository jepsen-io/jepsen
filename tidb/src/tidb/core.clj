(ns tidb.core
  "Runs TiDB tests. Provides exit status reporting."
  (:gen-class)
  (:refer-clojure :exclude [test])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen [cli :as jc]
                    [checker :as checker]
                    [core :as jepsen]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.os.debian :as debian]
            [tidb [bank :as bank]
                  [db :as db]
                  [nemesis :as nemesis]
                  [register :as register]
                  [sequential :as sequential]
                  [sets :as set]]))

(def oses
  "Supported operating systems"
  {"debian" debian/os
   "none"   os/noop})

(def workloads
  "A map of workload names to functions that can take CLI opts and construct
  workloads."
  {:bank            bank/workload
   :bank-multitable bank/multitable-workload
   :register        register/workload
   :set             set/workload
   :sequential      sequential/workload})

(def workload-options
  "For each workload, a map of workload options to all values that option
  supports."
  {:bank            {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :update-in-place   [true false]
                     :read-lock         [nil "FOR UPDATE"]}
   :bank-multitable {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :update-in-place   [true false]
                     :read-lock         [nil "FOR UPDATE"]}
   :register        {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :read-lock         [nil "FOR UPDATE"]}
   :set             {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]}
   :sequential      {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]}})

(def workload-options-expected-to-pass
  "Workload options restricted to only those we expect to pass."
  (-> (util/map-vals #(assoc %
                             :auto-retry        [false]
                             :auto-retry-limit  [0])
                     workload-options)))

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

(def nemesis-specs
  "These are the types of failures that the nemesis can perform."
  #{:inter-replica-partition
    :intra-replica-partition
    :single-node-partition
    :partition
    :kill
    :pause
    :kill-pd
    :kill-kv
    :kill-db
    :pause-pd
    :pause-kv
    :pause-db
    :clock-skew})

(def all-nemeses
  "All nemesis specs to run as a part of a complete test suite."
  (->> [; No faults
        []
        ; Single faults
        [:kill]
        [:pause]
        [:clock-skew]
        [:partitions]
        ; Combined
        [:kill :pause :clock-skew :partitions]]
       (map (fn [faults] (zipmap faults (repeat true))))))

(defn parse-version
  "Finds the version of TiDB from CLI opts by parsing :tarball"
  [opts]
  ((re-find #"v(\d+\.\d+\.\d+)" (:tarball opts)) 1))

(def plot-spec
  "Specification for how to render operations in plots"
  {:nemeses #{{:name        "kill pd"
               :color       "#E9A4A0"
               :start       #{:kill-pd}
               :stop        #{:start-pd}}
              {:name        "kill kv"
               :color       "#D1A0AB"
               :start       #{:kill-kv}
               :stop        #{:start-kv}}
              {:name        "kill db"
               :color       "#D3A4B5"
               :start       #{:kill-db}
               :stop        #{:start-db}}}})

(defn test
  "Constructs a test from a map of CLI options."
  [opts]
  (let [name (str "TiDB " (parse-version opts)
                  " " (name (:workload opts))
                  (when (:auto-retry opts)
                    " auto-retry ")
                  (when (not= 0 (:auto-retry-limit opts))
                    (str " auto-retry-limit " (:auto-retry-limit opts)))
                  (when (:update-in-place opts)
                    " update-in-place")
                  (when (:read-lock opts)
                    (str " select " (:read-lock opts)))
                  (when-not (= [:interval] (keys (:nemesis opts)))
                    (str " nemesis " (->> (dissoc (:nemesis opts)
                                                   :interval
                                                   :schedule
                                                   :long-recovery)
                                          keys
                                          (map name)
                                          sort
                                          (str/join ",")))))
        workload  ((get workloads (:workload opts)) opts)
        nemesis   (nemesis/nemesis opts)
        gen       (->> (:generator workload)
                       (gen/nemesis (:generator nemesis))
                       (gen/time-limit (:time-limit opts)))
        gen       (if (:final-generator workload)
                    (gen/phases gen
                                (gen/log "Healing cluster")
                                (gen/nemesis (:final-generator nemesis))
                                (gen/log "Waiting for recovery")
                                (gen/sleep (:final-recovery-time opts))
                                (gen/clients (:final-generator workload)))
                    gen)]
    (merge tests/noop-test
           opts
           (dissoc workload :final-generator)
           {:name       name
            :os         debian/os
            :db         (db/db)
            :client     (:client workload)
            :nemesis    (:nemesis nemesis)
            :generator  gen
            :plot       plot-spec
            :checker    (checker/compose
                          {:perf        (checker/perf)
                           :clock-skew  (checker/clock-plot)
                           :workload    (:checker workload)})})))

(defn parse-nemesis-spec
  "Parses a comma-separated string of nemesis types, and turns it into an
  option map like {:kill-alpha? true ...}"
  [s]
  (if (= s "none")
    {}
    (->> (str/split s #",")
         (map (fn [o] [(keyword o) true]))
         (into {}))))

(defn parse-long [x] (Long/parseLong x))

(def cli-opts
  "Command line options for tools.cli"
  [[nil "--nemesis-interval SECONDS"
    "Roughly how long to wait between nemesis operations. Default: 10s."
    :parse-fn parse-long
    :assoc-fn (fn [m k v] (update m :nemesis assoc :interval v))
    :validate [(complement neg?) "should be a non-negative number"]]

   [nil "--nemesis SPEC" "A comma-separated list of nemesis types"
    :default {:interval 10}
    :parse-fn parse-nemesis-spec
    :assoc-fn (fn [m k v] (update m :nemesis merge v))
    :validate [(fn [parsed]
                 (and (map? parsed)
                      (every? nemesis-specs (keys parsed))))
               (str "Should be a comma-separated list of failure types. A failure "
                    (.toLowerCase (jc/one-of nemesis-specs))
                    ". Or, you can use 'none' to indicate no failures.")]]

   [nil "--nemesis-long-recovery" "Every so often, have a long period of no faults, to see whether the cluster recovers."
    :default false
    :assoc-fn (fn [m k v] (update m :nemesis assoc :long-recovery v))]

   [nil "--nemesis-schedule SCHEDULE" "Whether to have randomized delays between nemesis actions, or fixed ones."
    :parse-fn keyword
    :assoc-fn (fn [m k v] (update m :nemesis assoc :schedule v))
    :validate [#{:fixed :random} "Must be either 'fixed' or 'random'"]]

   ["-o" "--os NAME" "debian, or none"
    :default debian/os
    :parse-fn oses
    :validate [identity (jc/one-of oses)]]

   [nil "--recovery-time SECONDS"
    "How long to wait for cluster recovery before final ops."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   (jc/tarball-opt "http://download.pingcap.org/tidb-v2.1.7-linux-amd64.tar.gz")])

(def test-all-opts
  "CLI options for running the entire test suite."
  [["-w" "--workload NAME"
    "Test workload to run. If omitted, runs all workloads"
    :parse-fn keyword
    :default nil
    :validate [workloads (jc/one-of workloads)]]

   [nil "--only-workloads-expected-to-pass"
    "If present, skips tests which are not expected to pass, given Fauna's docs"
    :default false]])

(def single-test-opts
  "CLI options for running a single test"
  [[nil "--auto-retry" "Enables automatic retries (the default for TiDB)"
    :default false]

   [nil "--auto-retry-limit COUNT" "How many automatic retries can we execute?"
    :default 10
    :parse-fn parse-long
    :validate [(complement neg?) "Must not be negative"]]

   [nil "--read-lock TYPE"
    "What kind of read locks, if any, should we acquire? Default is none; may
    also be 'update'."
    :default nil
    :parse-fn {"update" "FOR UPDATE"}
    :validate #{nil "FOR UPDATE"}]

   [nil "--update-in-place"
    "If true, performs updates (on some workloads) in place, rather than
    separating read and write operations."
    :default false]

   ["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (jc/one-of workloads))
    :validate [workloads (jc/one-of workloads)]]])

(defn test-all-cmd
  "A command that runs a whole suite of tests in one go."
  []
  {"test-all"
   {:opt-spec (concat jc/test-opt-spec cli-opts test-all-opts)
    :opt-fn   jc/test-opt-fn
    :usage    "Runs all combinations of workloads, nemeses, and options."
    :run      (fn [{:keys [options]}]
                (info "CLI options:\n" (with-out-str (pprint options)))
                (let [w         (:workload options)
                      workload-opts (if (:only-workloads-expected-to-pass options)
                                      workload-options-expected-to-pass
                                      workload-options)
                      workloads (cond->> (all-workload-options workload-opts)
                                  w (filter (comp #{w} :workload)))
                      tests (for [nemesis   all-nemeses
                                  workload  workloads
                                  i         (range (:test-count options))]
                              (do
                              (-> options
                                  (merge workload)
                                  (update :nemesis merge nemesis))))]
                  (->> tests
                       (map-indexed
                         (fn [i test-opts]
                           (try
                             (info "\n\n\nTest " (inc i) "/" (count tests))
                             (jepsen/run! (test test-opts))
                             (catch Exception e
                               (warn e "Test crashed; moving on...")))))
                       dorun)))}})

(defn -main
  [& args]
  (jc/run!
    (merge (jc/serve-cmd)
           (test-all-cmd)
           (jc/single-test-cmd {:test-fn  test
                                :opt-spec (concat cli-opts single-test-opts)}))
    args))
