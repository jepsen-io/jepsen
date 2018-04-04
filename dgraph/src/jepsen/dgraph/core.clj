(ns jepsen.dgraph.core
  (:gen-class)
  (:require [clojure.string :as str]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [jepsen.dgraph [bank :as bank]
                           [delete :as delete]
                           [long-fork :as long-fork]
                           [nemesis :as nemesis]
                           [sequential :as sequential]
                           [set :as set]
                           [support :as s]
                           [upsert :as upsert]]))

(def workloads
  "A map of workload names to functions that can take opts and construct
  workloads."
  {:bank        bank/workload
   :delete      delete/workload
   :long-fork   long-fork/workload
   :upsert      upsert/workload
   :set         set/workload
   :uid-set     set/uid-workload
   :sequential  sequential/workload})

(def nemesis-specs
  "These are the types of failures that the nemesis can perform"
  #{:kill-alpha?
    :kill-zero?
    :partition?})

(defn dgraph-test
  "Builds up a dgraph test map from CLI options."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)
        nemesis  (nemesis/nemesis (:nemesis opts))
        gen      (->> (:generator workload)
                      (gen/nemesis (:generator nemesis))
                      (gen/time-limit (:time-limit opts)))
        gen      (if (:final-generator workload)
                   (gen/phases gen
                               (gen/log "Healing cluster.")
                               (gen/nemesis (:final-generator nemesis))
                               (gen/log "Waiting for recovery.")
                               (gen/sleep 10)
                               (gen/clients (:final-generator workload)))
                   gen)]
    (merge tests/noop-test
           opts
           (dissoc workload :final-generator)
           {:name       (str "dgraph " (:version opts) " "
                             (name (:workload opts))
                             (:when (:upsert-schema opts) " @upsert"))
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
  "Additional command line options"
  [["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   [nil  "--nemesis SPEC" "A comma-separated list of nemesis types"
    :default "kill-alpha,kill-zero,partition"
    :parse-fn parse-nemesis-spec
    :validate [(fn [parsed]
                 (prn parsed)
                 (and (map? parsed)
                      (every? nemesis-specs (keys parsed))))
               (str "Should be a comma-separated list of failure types. A failure type "
                    (.toLowerCase (cli/one-of nemesis-specs))
                    ". Or, you can use 'none' to indicate no failures.")]]
   ["-v" "--version VERSION" "What version number of dgraph should we test?"
    :default "1.0.3"]
   [nil "--package-url URL" "Ignore version; install this tarball instead"
    :validate [(partial re-find #"\A(file)|(https?)://")
               "Should be an HTTP url"]]
   ["-f" "--force-download" "Ignore the package cache; download again."
    :default false]
   [nil "--upsert-schema"
    "If present, tests will use @upsert schema directives."
    :default false]
   [nil "--replicas COUNT" "How many replicas of data should dgraph store?"
    :default 3
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]])

(defn -main
  "Handles command line arguments; running tests or the web server."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   dgraph-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
