(ns jepsen.faunadb.runner
  "Runs FaunaDB tests. Provides exit status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [unilog.config :as unilog]
            [jepsen [cli :as jc]
                    [core :as jepsen]
                    [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]
                    [web :as web]]
            [jepsen.nemesis.time :as nt]
            [jepsen.os.debian :as debian]
            [jepsen.faunadb [auto :as auto]
                            [bank :as bank]
                            [g2 :as g2]
                            [register :as register]
                            [monotonic :as monotonic]
                            [multimonotonic :as multimonotonic]
                            [set :as set]
                            [pages :as pages]
                            [internal :as internal]
                            [nemesis :as nemesis]
                            [topology :as topo]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:set             set/workload
   :bank            bank/workload
   :bank-index      bank/index-workload
   :g2              g2/workload
   :internal        internal/workload
   :monotonic       monotonic/workload
   :multimonotonic  multimonotonic/workload
   :pages           pages/workload
   :register        register/workload})

(def workload-options
  "For each workload, a map of workload options to all the values that option
  supports."
  {:set         {:serialized-indices  [true false]
                 :strong-read         [true false]}
   :bank        {:fixed-instances     [true false]
                 :at-query            [true false]}
   :bank-index  {:fixed-instances     [true false]
                 :serialized-indices  [true false]}
   :g2          {:serialized-indices  [true false]}
   :internal    {:serialized-indices  [true false]}
   :monotonic   {:at-query-jitter     [0 10000 100000]}
   :multimonotonic {}
   :pages       {:serialized-indices  [true false]}
   :register    {}})

(def workload-options-expected-to-pass
  "Workload options restricted to just those we expect to pass."
  (-> workload-options
      (assoc-in [:set :strong-read]       [true])
      ; I forget, are serialized indices necessary for set tests?
      (assoc-in [:set :serialized-indices] [true])
      (assoc-in [:g2 :serialized-indices] [true])))

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
    :kill
    :stop
    :topology
    :clock-skew})

(def all-nemeses
  "All nemesis specs to run as a part of a complete test suite"
  (->> [; No faults
        []
        ; Single types of faults
        [:kill]
        [:stop]
        [:clock-skew]
        ; Partitions
        [:inter-replica-partition
         :intra-replica-partition
         :single-node-partition]
        ; Everything but topology
        [:inter-replica-partition
         :intra-replica-partition
         :single-node-partition
         :clock-skew
         :kill
         :stop]
        [:topology]]
       (map (fn [faults]
              (zipmap faults (repeat true))))))

(defn parse-version
  "Handle local package files by ignoring the path"
  [opts]
  (let [v (:version opts)]
    (last (str/split v #"/"))))

(defn test-1
  "Initial test construction from a map of CLI options. Establishes the test
  name, OS, DB, and topology."
  [opts]
  (assoc opts
         :name (str "fauna " (parse-version opts)
                    " " (name (:workload opts))
                    (when (:strong-read opts)
                      " strong-read")
                    (when (:at-query opts)
                      " at-query")
                    (when-let [j (:at-query-jitter opts)]
                      (str " jitter " j))
                    (when (:fixed-instances opts)
                      " fixed-instances")
                    (when (:serialized-indices opts)
                      " serialized-indices")
                    (when-not (= [:interval] (keys (:nemesis opts)))
                      (str " nemesis " (->> (dissoc (:nemesis opts) :interval)
                                            keys
                                            (map name)
                                            sort
                                            (str/join ",")))))
         :os debian/os
         :db (auto/db)
         :topology (atom (topo/initial-topology opts))
         :nonserializable-keys [:topology]))

(defn test-2
  "Second phase of test construction. Builds the workload and nemesis, and
  finalizes the test."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        nemesis  (nemesis/nemesis (:nemesis opts))
        gen      (->> (:generator workload)
                      (gen/nemesis (:generator nemesis))
                      (gen/time-limit (:time-limit opts)))
        gen (if (:final-generator workload)
              (gen/phases gen
                          (gen/log "Healing cluster")
                          (gen/nemesis (:final-generator nemesis))
                          (gen/log "Waiting for recovery.")
                          (gen/sleep (:final-recovery-time opts))
                          (gen/clients (:final-generator workload)))
              gen)]
    (merge tests/noop-test
           opts
           (dissoc workload :final-generator)
           {:client (:client workload)
            :nemesis (:nemesis nemesis)
            :generator gen
            :checker   (checker/compose {:perf        (checker/perf)
                                         :clock-skew  (checker/clock-plot)
                                         :workload    (:checker workload)})})))

(defn fauna-test
  "Constructs a FaunaDB test map from CLI options."
  [opts]
  (-> opts test-1 test-2))

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
  "Options for single or multiple tests"
  [[nil "--clear-cache"
    "Clear the fast-startup cache and force a rebuild of the cluster"
    :default false]

   [nil "--datadog-api-key KEY"
    "If provided, sets up Fauna's integrated datadog stats"]

   [nil "--final-recovery-time SECONDS" "How long to wait for the cluster to stabilize at the end of a test"
    :default 10
    :parse-fn parse-long
    :validate [(complement neg?) "Must be a non-negative number"]]

   [nil "--nemesis-interval SECONDS"
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

   [nil "--wait-for-convergence" "Don't start operations until data movement has completed"
    :default false]

   [nil "--accelerate-indexes" "Use FaunaDB accelerated index builds."
    :default false]

   ["-r" "--replicas NUM" "Number of replicas"
    :default 3
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--version STRING" "Version of FaunaDB to install"
    :default "2.5.5"]])

(def test-all-opts
  "Command line options for testing everything"
  [["-w" "--workload NAME"
    "Test workload to run. If omitted, runs all workloads"
    :parse-fn keyword
    :default nil
    :validate [workloads (jc/one-of workloads)]]

   [nil "--only-workloads-expected-to-pass"
    "If present, skips tests which are not expected to pass, given Fauna's docs"
    :default false]])

(def single-test-opts
  "Command line options for single tests"
  [[nil "--strong-read" "Force strict reads by performing dummy writes"
    :default false]

   [nil "--at-query" "Use At queries for certain operations, rather than just reading."
    :default false]

   [nil "--at-query-jitter MS" "Time to jitter at queries, in milliseconds."
    :default 10000
    :parse-fn parse-long
    :validate [(complement neg?) "Must be a positive integer"]]

   [nil "--fixed-instances" "Don't create and destroy instances dynamically."
    :default false]

   [nil "--serialized-indices" "Use strict serializable indexes"
    :default false]

   ["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (jc/one-of workloads))
    :validate [workloads (jc/one-of workloads)]]])

(defn validate-replicas
  "Check that we have enough nodes to support a given replica count"
  [parsed]
  (let [opts (:options parsed)]
    (if (< (count (:nodes opts)) (:replicas opts))
      (update parsed :errors conj
              (str "Fewer nodes (" (:nodes opts)
                   ") than replicas (" (:replicas opts) ")"))
      parsed)))

(defn test-all-cmd
  "A command that runs a whole suite of tests in one go."
  []
  {"test-all"
   {:opt-spec (concat jc/test-opt-spec cli-opts test-all-opts)
    :opt-fn   jc/test-opt-fn
    :usage    "TODO"
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
                             (jepsen/run! (fauna-test test-opts))
                             (catch Exception e
                               (warn e "Test crashed; moving on...")))))
                       dorun)))}})

(defn -main
  [& args]
  ; Don't log every http error plz
  (unilog/start-logging! {:level     :info
                          :overrides {"com.faunadb.common.Connection" :error}})
  (jc/run! (merge (jc/serve-cmd)
                  (test-all-cmd)
                  (jc/single-test-cmd {:test-fn  fauna-test
                                       :opt-spec (concat cli-opts
                                                         single-test-opts)
                                       :opt-fn (fn [parsed]
                                                 (-> parsed
                                                     validate-replicas))}))
           args))
