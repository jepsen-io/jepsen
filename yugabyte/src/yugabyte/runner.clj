(ns yugabyte.runner
  "Runs YugaByteDB tests."
  (:gen-class)
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer :all]
            [jepsen [core :as jepsen]
                    [cli :as cli]]
            [yugabyte [core :as core]
                      [nemesis :as nemesis]]))

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

; Options
(def cli-opts
  "Options for single or multiple tests."
  [["-o" "--os NAME" "Operating system: either centos or debian."
    :default  :centos
    :parse-fn keyword
    :validate [#{:centos :debian} "One of `centos` or `debian`"]]

   ["-d" "--db NAME" "Database variant: either community-edition (ce for short), or enterprise edition (ee for short)"
    :default :community-edition
    :parse-fn {"ce"                 :community-edition
               "community-edition"  :community-edition
               "ee"                 :enterprise-edition
               "enterprise-edition" :enterprise-edition}
    :validate [#{:community-edition :enterprise-edition}
               "Either community-edition or enterprise edition"]]

   [nil "--final-recovery-time SECONDS" "How long to wait for the cluster to stabilize at the end of a test"
    :default 30
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
                      (every? core/nemesis-specs (keys parsed))))
               (str "Should be a comma-separated list of failure types. A failure "
                    (.toLowerCase (cli/one-of core/nemesis-specs))
                    ". Or, you can use 'none' to indicate no failures.")]]

   ["-r" "--replication-factor INT" "Number of nodes in each Raft cluster."
    :default 3
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive integer"]]

   [nil "--yugabyte-ssh" "Override SSH options with hardcoded defaults for Yugabyte's internal testing environment"
    :default false]

   [nil "--version VERSION" "What version of Yugabyte to install"
    :default "1.1.9.0"]

   [nil "--trace-cql" "If provided, logs CQL queries"
    :default false]])

(def test-all-opts
  "CLI options for testing everything."
  [["-w" "--workload NAME"
    "Test workload to run. If omitted, runs all workloads"
    :parse-fn keyword
    :default nil
    :validate [core/workloads (cli/one-of core/workloads)]]])

(def single-test-opts
	"Command line options for single tests"
	[["-w" "--workload NAME" "Test workload to run"
		:parse-fn keyword
		:missing (str "--workload " (cli/one-of core/workloads))
		:validate [core/workloads (cli/one-of core/workloads)]]])

(defn log-test
  [t attempt]
  (info "Testing" (:name t) "attempt #" attempt)
  t)

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
											workload-opts core/workload-options
											workloads (cond->> (core/all-workload-options
                                           workload-opts)
																	w (filter (comp #{w} :workload)))
											tests (for [nemesis   core/all-nemeses
                                  workload  workloads
																	i         (range (:test-count options))]
                              (-> options
                                  (merge workload)
                                  (update :nemesis merge nemesis)))]
									(->> tests
											 (map-indexed
												 (fn [i test-opts]
													 (try
														 (info "\n\n\nTest " (inc i) "/" (count tests))
														 (jepsen/run! (core/yb-test test-opts))
														 (catch Exception e
															 (warn e "Test crashed; moving on...")))))
											 dorun)))}})

(defn -main
  "Handles CLI arguments"
  [& args]
  (cli/run! (merge (cli/serve-cmd)
                   (test-all-cmd)
                   (cli/single-test-cmd {:test-fn  core/yb-test
                                         :opt-spec (concat cli-opts
                                                           single-test-opts)}))
            args))

(comment
  ; TODO: port the "following tests have been failed" logic forward.
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing results."
  [& args]
  (cli/run!
   (merge
    (merge-with merge
                (cli/single-test-cmd
                 {:opt-spec opt-spec
                  ; :test-fn is required by single-test-cmd to construct :run, but :run will be overridden below in
                  ; order to support running multiple tests.
                  :test-fn  yugabyte.core/yugabyte-test})
                {"test" {:run (fn [{:keys [options]}]
                                (info "Options:\n" (with-out-str (pprint options)))
                                (let [invalid-results
                                      (->>
                                       (for [i       (range 1 (inc (:test-count options)))
                                             test-fn (:test options)]
                                         (let [_ (info :i i)
                                               test (-> options
                                                        (dissoc :test)
                                                        (assoc :nemesis-name (:nemesis options))
                                                        test-fn
                                                        (log-test i)
                                                        jepsen/run!)]
                                           [(:results test) i]))
                                       (filter #(->> % first :valid? true? not)))]
                                  (info :invalid-results invalid-results)
                                  (when-not (empty? invalid-results)
                                    ((info "Following tests have been failed:\n" (with-out-str (pprint invalid-results)))
                                      (System/exit 1)))
                                  ))}})
    (cli/serve-cmd))
   args)))
