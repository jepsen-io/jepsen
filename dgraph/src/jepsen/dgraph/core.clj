(ns jepsen.dgraph.core
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn error]]
            [clojure.java.shell :refer [sh]]
            [clojure.pprint :refer [pprint]]
            [jepsen [cli :as cli]
                    [core :as jepsen]
                    [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [jepsen.dgraph [bank :as bank]
                           [delete :as delete]
                           [long-fork :as long-fork]
                           [linearizable-register :as lr]
                           [nemesis :as nemesis]
                           [sequential :as sequential]
                           [set :as set]
                           [support :as s]
                           [types :as types]
                           [upsert :as upsert]
                           [trace  :as t]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:bank                      bank/workload
   :delete                    delete/workload
   :long-fork                 long-fork/workload
   :linearizable-register     lr/workload
   :uid-linearizable-register lr/uid-workload
   :upsert                    upsert/workload
   :set                       set/workload
   :uid-set                   set/uid-workload
   :sequential                sequential/workload
   :types                     types/workload})

(def nemesis-specs
  "These are the types of failures that the nemesis can perform"
  #{:kill-alpha?
    :kill-zero?
    :fix-alpha?
    :partition-halves?
    :partition-ring?
    :move-tablet?
    :skew-clock?})

(def skew-specs
  #{:tiny
    :small
    :big
    :huge})

(defn dgraph-test
  "Builds up a dgraph test map from CLI options."
  [opts]
  (let [version  (if (:local-binary opts)
                   (let [v (:out (sh (:local-binary opts) "version"))]
                     (if-let [m (re-find #"Dgraph version   : (v[0-9a-z\.-]+)" v)]
                       (m 1)
                       "unknown"))
                   (if-let [p (:package-url opts)]
                     (if-let [m (re-find #"([^/]+)/[^/.]+\.tar\.gz" p)]
                       (m 1)
                       "unknown")
                     (:version opts)))
        workload ((get workloads (:workload opts)) opts)
        nemesis  (nemesis/nemesis (:nemesis opts))
        gen      (->> (:generator workload)
                      (gen/nemesis (:generator nemesis))
                      (gen/time-limit (:time-limit opts)))
        gen      (if (:final-generator workload)
                   (gen/phases gen
                               (gen/log "Healing cluster.")
                               (gen/nemesis (:final-generator nemesis))
                               (gen/log "Waiting for recovery.")
                               (gen/sleep (:final-recovery-time opts))
                               (gen/clients (:final-generator workload)))
                   gen)
        tracing (t/tracing (:tracing opts))]
    (merge tests/noop-test
           opts
           (dissoc workload :final-generator)
           {:name       (str "dgraph " version " "
                             (name (:workload opts))
                             (when (:upsert-schema opts) " upsert")
                             " nemesis="
                             (->> (dissoc (:nemesis opts) :interval)
                                  (map #(->> % key name butlast (apply str)))
                                  (str/join ",")))
            :version    version
            :os         debian/os
            :db         (s/db opts)
            :generator  gen
            :client     (:client workload)
            :nemesis    (:nemesis nemesis)
            :checker    (checker/compose
                          {:perf     (checker/perf)
                           :workload (:checker workload)})
            :tracing tracing})))

(defn parse-long [x] (Long/parseLong x))

(defn parse-nemesis-spec
  "Parses a comma-separated string of nemesis types, and turns it into an
  option map like {:kill-alpha? true ...}"
  [s]
  (if (= s "none")
    {}
    (->> (str/split s #",")
         (map (fn [o] [(keyword (str o "?")) true]))
         (into {}))))

(def cli-opts
  "Options for single and multiple tests"
  [["-v" "--version VERSION" "What version number of dgraph should we test?"
    :default "1.0.3"]
   [nil "--package-url URL" "Ignore version; install this tarball instead"
    :validate [(partial re-find #"\A(file)|(https?)://")
               "Should be an HTTP url"]]
   [nil "--local-binary PATH"
    "Ignore version and package; upload this local binary instead"]
   [nil "--replicas COUNT" "How many replicas of data should dgraph store?"
    :default 3
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]
   [nil "--rebalance-interval TIME" "How long before automatic rebalances"
    :default "10s"]
   [nil "--final-recovery-time SECONDS" "How long to wait for the cluster to stabilize at the end of a test"
    :default 10
    :parse-fn parse-long
    :validate [(complement neg?) "Must be a non-negative number"]]
   [nil "--retry-db-setup" "Work around Dgraph cluster convergence bugs by retrying the setup process"
    :default false]
   [nil "--tracing URL" "Enables tracing by providing an endpoint to export traces. Jaeger example: http://host.docker.internal:14268/api/traces"]
   [nil "--dgraph-jaeger-collector COLLECTOR" "Jaeger collector URL to pass to dgraph on startup."]
   [nil "--dgraph-jaeger-agent AGENT" "Jaeger agent URL to pass to dgraph on startup."]])

(def single-test-opts
  "Additional command line options for single tests"
  [["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   [nil "--nemesis-interval SECONDS"
    "Roughly how long to wait between nemesis operations."
    :default  10
    :parse-fn parse-long
    :assoc-fn (fn [m k v] (update m :nemesis assoc :interval v))
    :validate [(complement neg?) "should be a non-negative number"]]
   [nil  "--nemesis SPEC" "A comma-separated list of nemesis types"
    :default {:interval 10}
    :parse-fn parse-nemesis-spec
    :assoc-fn (fn [m k v]
                (update m :nemesis merge v))
    :validate [(fn [parsed]
                 (and (map? parsed)
                      (every? nemesis-specs (keys parsed))))
               (str "Should be a comma-separated list of failure types. A failure type "
                    (.toLowerCase (cli/one-of nemesis-specs))
                    ". Or, you can use 'none' to indicate no failures.")]]
   [nil "--skew SPEC" "Set the duration of clock skews"
    :parse-fn keyword
    :default :small
    :assoc-fn (fn [m k v] (update m :nemesis assoc :skew v))
    :validate [skew-specs (.toLowerCase (cli/one-of skew-specs))]]
   ["-f" "--force-download" "Ignore the package cache; download again."
    :default false]
   [nil "--upsert-schema"
    "If present, tests will use @upsert schema directives. To disable, provide false"
    :parse-fn (complement #{"false"})
    :default true]
   ["-s" "--sequencing MODE" "DEPRECATED: --sequencing flag provided. This will be removed in a future version as server/client sequencing is no longer supported by Dgraph."]
   [nil "--defer-db-teardown" "Wait until user input to tear down DB nodes"
    :default false]])

(defn test-all-cmd
  "A command to run a whole suite of tests in one go."
  []
  (let [opt-spec (into cli/test-opt-spec cli-opts)]
    {"test-all"
     {:opt-spec opt-spec
      :opt-fn   cli/test-opt-fn
      :usage    "TODO"
      :run       (fn [{:keys [options]}]
                   (info "CLI options:\n" (with-out-str (pprint options)))
                   (let [force-download? (atom true)
                         tests (for [i          (range (:test-count options))
                                     workload   (remove #{:types :uid-set}
                                                        (keys workloads))
                                     upsert     [true]
                                     nemesis    [; Nothing
                                                 {:interval         1}
                                                 ; Predicate migrations
                                                 {:interval         15
                                                  :move-tablet?     true}
                                                 ; Partitions
                                                 {:interval         30
                                                  :partition-ring?  true}
                                                 ; Process kills
                                                 {:interval         30
                                                  :kill-alpha?      true
                                                  :kill-zero?       true}
                                                 ; Everything
                                                 {:interval         30
                                                  :move-tablet?     true
                                                  :partition-ring?  true
                                                  :kill-alpha?      true
                                                  :kill-zero?       true}]]
                                 (assoc options
                                        :workload       workload
                                        :upsert-schema  upsert
                                        :nemesis        nemesis
                                        :force-download @force-download?))]
                     (->> tests
                          (map-indexed
                            (fn [i test-opts]
                              (try
                                (info "\n\n\nTest" (inc i) "/" (count tests))
                                ; Run test
                                (jepsen/run! (dgraph-test test-opts))
                                ; We've run once, no need to download
                                ; again
                                (reset! force-download? false)
                                (catch Exception e
                                  (warn e "Test crashed; moving on...")))))
                          dorun)))}}))


(defn -main
  "Handles command line arguments; running tests or the web server."
  [& args]
  (cli/run! (merge (test-all-cmd)
                   (cli/single-test-cmd {:test-fn   dgraph-test
                                         :opt-spec  (concat cli-opts
                                                            single-test-opts)})
                   (cli/serve-cmd))
            args))
