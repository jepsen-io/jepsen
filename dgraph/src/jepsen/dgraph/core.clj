(ns jepsen.dgraph.core
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
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
                           [upsert :as upsert]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:bank                  bank/workload
   :delete                delete/workload
   :long-fork             long-fork/workload
   :linearizable-register lr/workload
   :upsert                upsert/workload
   :set                   set/workload
   :uid-set               set/uid-workload
   :sequential            sequential/workload
   :types                 types/workload})

(def nemesis-specs
  "These are the types of failures that the nemesis can perform"
  #{:kill-alpha?
    :kill-zero?
    :fix-alpha?
    :partition?
    :move-tablet?})

(defn dgraph-test
  "Builds up a dgraph test map from CLI options."
  [opts]
  (let [version  (if (:local-binary opts)
                   "unknown"
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
                   gen)]
    (merge tests/noop-test
           opts
           (dissoc workload :final-generator)
           {:name       (str "dgraph " version " "
                             (name (:workload opts))
                             " s=" (name (:sequencing opts))
                               (when (:upsert-schema opts) " upsert")
                             " nemesis="
                             (->> (dissoc (:nemesis opts) :interval)
                                  (map #(->> % key name butlast (apply str)))
                                  (str/join ",")))
            :version    version
            :os         debian/os
            :db         (s/db)
            :generator  gen
            :client     (:client workload)
            :nemesis    (:nemesis nemesis)
            :checker    (checker/compose
                          {:perf     (checker/perf)
                           :workload (:checker workload)})})))

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
   [nil "--final-recovery-time SECONDS" "How long to wait for the cluster to stabilize at the end of a test"
    :default 10
    :parse-fn parse-long
    :validate [(complement neg?) "Must be a non-negative number"]]])

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
   ["-f" "--force-download" "Ignore the package cache; download again."
    :default false]
   [nil "--upsert-schema"
    "If present, tests will use @upsert schema directives."
    :default false]
   ["-s" "--sequencing MODE" "Whether to use server or client side sequencing"
    :default :server
    :parse-fn keyword
    :validate [#{:client :server} "Must be either `client` or `server`."]]])

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
                                     workload   (remove #{:types}
                                                        (keys workloads))
                                     sequencing [:client :server]
                                     upsert     [false true]
                                     nemesis    [; Nothing
                                                 {:interval 1}
                                                 ; Predicate migrations
                                                 {:interval     5
                                                  :move-tablet? true}
                                                 ; Partitions
                                                 {:interval     60
                                                  :partition?   true}
                                                 ; Process kills
                                                 {:interval     30
                                                  :kill-alpha?  true
                                                  :kill-zero?   true}
                                                 ; Everything
                                                 {:interval     30
                                                  :move-tablet? true
                                                  :partition?   true
                                                  :kill-alpha?  true
                                                  :kill-zero?   true}]]
                                 (assoc options
                                        :workload       workload
                                        :sequencing     sequencing
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
