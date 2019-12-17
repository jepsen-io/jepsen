(ns jepsen.consul
  (:gen-class)
  (:use jepsen.core
        jepsen.tests
        clojure.test
        clojure.pprint)
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen.core :as core]
            [jepsen.util :as util :refer [meh timeout]]
            [jepsen.control :as c]
            [jepsen.cli :as cli]
            [jepsen.os.debian :as debian]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.store :as store]
            [jepsen.report :as report]
            [jepsen.tests :as tests]
            [knossos.model :as model]
            [jepsen.consul.client :as cc]
            [jepsen.consul.db :as db]))

;; TODO Port this to jepsen.tests.linearizable_register
(defn register-test
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         opts
         ;; TODO Add test name once workloads are implemented
         {:name (str "consul " (:version opts))
          :os debian/os
          :initialized? (atom false)
          :db (db/db)
          :client (cc/cas-client)
          ;; TODO Lift this to an independent key checker
          :checker (checker/compose
                    {:perf     (checker/perf)
                     :timeline (timeline/html)
                     :linear (checker/linearizable
                              {:model (model/cas-register)})})
          :nemesis   (nemesis/partition-random-halves)
          :generator (gen/phases
                      (->> gen/cas
                           (gen/delay 1/2)
                           (gen/nemesis
                            (gen/seq
                             (cycle [(gen/sleep 10)
                                     {:type :info :f :start}
                                     (gen/sleep 10)
                                     {:type :info :f :stop}])))
                           (gen/time-limit (or (:time-limit opts) 120)))
                      (gen/nemesis
                       (gen/once {:type :info :f :stop}))
                      ;; (gen/sleep 10)
                      (gen/clients
                       (gen/once {:type :invoke :f :read})))}))

;; TODO Port reusable components from register-test over to consul-test and implement worklaods
(defn consul-test
  [opts])

(def cli-opts
  "Additional command line options."
  [["-v" "--version STRING" "What version of etcd should we install?"
    :default "1.6.1"]
   #_["-w" "--workload NAME" "What workload should we run?"
    :missing  (str "--workload " (cli/one-of workloads))
    :validate [workloads (cli/one-of workloads)]]
   #_["-s" "--serializable" "Use serializable reads, instead of going through consensus."]
   #_["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   #_[nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  200
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]])

;; TODO Migrate to a runner.clj and cli-opts
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd
                    {:test-fn register-test
                     :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))

