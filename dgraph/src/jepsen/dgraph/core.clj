(ns jepsen.dgraph.core
  (:require [jepsen [cli :as cli]
                    [nemesis :as nemesis]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.dgraph [support :as s]
                           [bank :as bank]]))

(def workloads
  {:bank bank/workload})

(defn dgraph-test
  "Builds up a dgraph test map from CLI options."
  [opts]
  (let [workload ((get workloads (:workload opts)) opts)]
    (merge tests/noop-test
           opts
           workload
           {:name (str "dgraph " (:version opts) " " (name (:workload opts)))
            :generator (->> (:generator workload)
                            (gen/stagger 5)
                            (gen/nemesis nil)
                            (gen/time-limit (:time-limit opts)))
            :os   debian/os
            :db   (s/db)})))

(defn parse-long [x] (Long/parseLong x))

(def cli-opts
  "Additional command line options"
  [["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   ["-v" "--version VERSION" "What version number of dgraph should we test?"
    :default "1.0.3"]
   [nil "--replicas COUNT" "How many replicas of data should dgraph store?"
    :default 3
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])

(defn -main
  "Handles command line arguments; running tests or the web server."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   dgraph-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
