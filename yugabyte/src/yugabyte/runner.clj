(ns yugabyte.runner
  "Runs YugaByteDB tests."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.cli :as cli]
            [jepsen.store :as store]
            [yugabyte.core :as core]
            [yugabyte.nemesis :as nemesis]))

(defn parse-long [x] (Long/parseLong x))

(defn parse-nemesis-spec
  "Parses a comma-separated string of nemesis types, and turns it into an
  option map like {:kill-alpha? true ...}"
  [s]
  (if (= s "none")
    {}
    (->> (str/split s #",")
         (map (fn [o] [(keyword o) true]))
         (into {}))))

(defn one-of
  "Like jepsen.cli/one-of but doesn't 'eat' namespaces"
  [coll]
  (let [stringify      (fn [s] (if (qualified-keyword? s)
                                 (str (namespace s) "/" (name s))
                                 (name s)))
        coll-keys      (if (map? coll) (keys coll) coll)
        coll-key-names (sort (map stringify coll-keys))]
    (str "Must be one of " (str/join ", " coll-key-names))))


(defn log-test
  [t attempt]
  (info "Testing" (:name t) "attempt #" attempt)
  t)

;
; Options
;
; For the options format, see clojure.tools.cli/parse-opts
;

(def cli-opts
  "Options for single or multiple tests."
  [["-o" "--os NAME" "Operating system: either centos or debian."
    :default :centos
    :parse-fn keyword
    :validate [#{:centos :debian} "One of `centos` or `debian`"]]

   [nil "--experimental-tuning-flags" "Enable some experimental tuning flags which are supposed to help YB recover faster"
    :default false]

   [nil "--final-recovery-time SECONDS" "How long to wait for the cluster to stabilize at the end of a test"
    :default 30
    :parse-fn parse-long
    :validate [(complement neg?) "Must be a non-negative number"]]

   [nil "--nemesis SPEC" "A comma-separated list of nemesis types"
    :default {:interval 10}
    :parse-fn parse-nemesis-spec
    :assoc-fn (fn [m k v] (update m :nemesis merge v))
    :validate [(fn [parsed]
                 (and (map? parsed)
                      (every? core/nemesis-specs (keys parsed))))
               (str "Should be a comma-separated list of failure types. A failure "
                    (.toLowerCase (cli/one-of core/nemesis-specs))
                    ". Or, you can use 'none' to indicate no failures.")]]

   [nil "--nemesis-interval SECONDS"
    "Roughly how long to wait between nemesis operations. Default: 10s."
    :parse-fn parse-long
    :assoc-fn (fn [m k v] (update m :nemesis assoc :interval v))
    :validate [(complement neg?) "should be a non-negative number"]]

   [nil "--nemesis-long-recovery" "Every so often, have a long period of no faults, to see whether the cluster recovers."
    :default false
    :assoc-fn (fn [m k v] (update m :nemesis assoc :long-recovery v))]

   [nil "--nemesis-schedule SCHEDULE" "Whether to have randomized delays between nemesis actions, or fixed ones."
    :parse-fn keyword
    :assoc-fn (fn [m k v] (update m :nemesis assoc :schedule v))
    :validate [#{:fixed :random} "Must be either 'fixed' or 'random'"]]

   ["-r" "--replication-factor INT" "Number of nodes in each Raft cluster."
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive integer"]]

   [nil "--yugabyte-ssh" "Override SSH options with hardcoded defaults for Yugabyte's internal testing environment"
    :default false]

   [nil "--version VERSION" "What version of Yugabyte to install"
    :default "1.3.1.0"]

   [nil "--table-count INT" "Number of tables to spread rows across."
    :default 5]

   [nil "--url URL" "URL to Yugabyte tarball to install, has precedence over --version"
    :default nil]

   [nil "--trace-cql" "If provided, logs CQL queries"
    :default false]])

(def test-all-opts
  "CLI options for testing everything."
  [[nil "--only-workloads-expected-to-pass" "If present, skips tests which are not expected to pass"
    :default false]

   ["-w" "--workload NAME"
    "Test workload to run. If omitted, runs all workloads"
    :parse-fn keyword
    :default nil
    :validate [core/workloads (one-of core/workloads)]]])

(def single-test-opts
  "Command line options for single tests"
  [["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (one-of core/workloads))
    :validate [core/workloads (one-of core/workloads)]]])

;
; Subcommands
;

(defn test-all-cmd
  "A command that runs a whole suite of tests in one go."
  []
  {"test-all"
   {:opt-spec (concat cli/test-opt-spec cli-opts test-all-opts)
    :opt-fn   cli/test-opt-fn
    :usage    "Runs all tests"
    :run      (fn [{:keys [options]}]
                (info "CLI options:\n" (with-out-str (pprint options)))
                (let [w             (:workload options)
                      workload-opts (if (:only-workloads-expected-to-pass options)
                                      core/workload-options-expected-to-pass
                                      core/workload-options)
                      workloads     (cond->> (core/all-workload-options
                                               workload-opts)
                                             w (filter (comp #{w} :workload)))
                      tests         (for [nemesis  core/all-nemeses
                                          workload workloads
                                          i        (range (:test-count options))]
                                      (-> options
                                          (merge workload)
                                          (update :nemesis merge nemesis)))
                      results       (->> tests
                                         (map-indexed
                                           (fn [i test-opts]
                                             (let [test (core/yb-test test-opts)]
                                               (try
                                                 (info "\n\n\nTest "
                                                       (inc i) "/" (count tests))
                                                 (let [test' (jepsen/run! test)]
                                                   [(.getPath (store/path test'))
                                                    (:valid? (:results test'))])
                                                 (catch Exception e
                                                   (warn e "Test crashed")
                                                   [(:name test) :crashed])))))
                                         (group-by second))]

                  (println "\n")

                  (when (seq (results true))
                    (println "\n# Successful tests\n")
                    (dorun (map (comp println first) (results true))))

                  (when (seq (results :unknown))
                    (println "\n# Indeterminate tests\n")
                    (dorun (map (comp println first) (results :unknown))))

                  (when (seq (results :crashed))
                    (println "\n# Crashed tests\n")
                    (dorun (map (comp println first) (results :crashed))))

                  (when (seq (results false))
                    (println "\n# Failed tests\n")
                    (dorun (map (comp println first) (results false))))

                  (println)
                  (println (count (results true)) "successes")
                  (println (count (results :unknown)) "unknown")
                  (println (count (results :crashed)) "crashed")
                  (println (count (results false)) "failures")))}})

(defn -main
  "Handles CLI arguments"
  [& args]
  (cli/run! (merge (cli/serve-cmd)
                   (test-all-cmd)
                   (cli/single-test-cmd {:test-fn  core/yb-test
                                         :opt-spec (concat cli-opts
                                                           single-test-opts)}))
            args))
