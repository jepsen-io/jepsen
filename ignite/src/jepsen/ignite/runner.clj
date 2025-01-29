(ns jepsen.ignite.runner
  "Runs Apache Ignite tests."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [jepsen.cli :as jc]
            [jepsen.core :as jepsen]
            [jepsen.ignite  [register :as register]
                            [bank     :as bank]
                            [nemesis  :as nemesis]])
  (:import (org.apache.ignite.cache CacheMode CacheAtomicityMode CacheWriteSynchronizationMode)
           (org.apache.ignite.transactions TransactionConcurrency TransactionIsolation)))

(def tests
  "A map of test names to test constructors."
  {"bank"     bank/test
   "register" register/test})

(def cache-modes
  {"PARTITIONED" CacheMode/PARTITIONED
   "REPLICATED"  CacheMode/REPLICATED
   "LOCAL"       CacheMode/LOCAL})

(def cache-atomicity-modes
  {"TRANSACTIONAL"          CacheAtomicityMode/TRANSACTIONAL
   "ATOMIC"                 CacheAtomicityMode/ATOMIC
   "TRANSACTIONAL_SNAPSHOT" CacheAtomicityMode/TRANSACTIONAL_SNAPSHOT})

(def cache-write-sync-modes
  {"FULL_SYNC"    CacheWriteSynchronizationMode/FULL_SYNC
   "PRIMARY_SYNC" CacheWriteSynchronizationMode/PRIMARY_SYNC
   "FULL_ASYNC"   CacheWriteSynchronizationMode/FULL_ASYNC})

(def transaction-concurrency-modes
  {"OPTIMISTIC"  TransactionConcurrency/OPTIMISTIC
   "PESSIMISTIC" TransactionConcurrency/PESSIMISTIC})

(def transaction-isolation-modes
  {"SERIALIZABLE"    TransactionIsolation/SERIALIZABLE
   "READ_COMMITTED"  TransactionIsolation/READ_COMMITTED
   "REPEATABLE_READ" TransactionIsolation/REPEATABLE_READ})

(def nemesis-types
  {"noop"                   jepsen.nemesis/noop
  "partition-random-halves" nemesis/partition-random-halves
  "kill-node"               nemesis/kill-node})

(def opt-spec
  "Command line options for tools.cli"
  [(jc/repeated-opt "-t" "--test NAME" "Test(s) to run" [] tests)
   [nil
   "--cache-mode CacheMode"
    "Which cache behaviour to use."
    :default  CacheMode/PARTITIONED
    :parse-fn cache-modes
    :validate [identity (jc/one-of cache-modes)]]
   [nil
    "--cache-atomicity-mode CacheAtomicityMode"
    "Whether cache should maintain fully transactional semantics or more light-weight atomic behavior."
    :default  CacheAtomicityMode/TRANSACTIONAL
    :parse-fn cache-atomicity-modes
    :validate [identity (jc/one-of cache-atomicity-modes)]]
   [nil
    "--cache-write-sync-mode CacheWriteSynchronizationMode"
    "Whether Ignite should wait for write replies from other nodes."
    :default  CacheWriteSynchronizationMode/FULL_SYNC
    :parse-fn cache-write-sync-modes
    :validate [identity (jc/one-of cache-write-sync-modes)]]
   [nil
    "--read-from-backup ReadFromBackup"
    "Whether data can be read from backup."
    :default  :true
    :parse-fn str
    :validate [#{"true" "false"} "One of `true` or `false`"]]
   [nil
    "--transaction-concurrency TransactionConcurrency"
    "Which transaction concurrency control to use."
    :default  TransactionConcurrency/PESSIMISTIC
    :parse-fn transaction-concurrency-modes
    :validate [identity (jc/one-of transaction-concurrency-modes)]]
   [nil
    "--transaction-isolation TransactionIsolation"
    "Which cache transaction isolation levels to use."
    :default  TransactionIsolation/REPEATABLE_READ
    :parse-fn transaction-isolation-modes
    :validate [identity (jc/one-of transaction-isolation-modes)]]
   [nil "--backups Backups"
    "How many backups to use."
    :default 2
    :parse-fn #(Long/parseLong %)
    :validate [nat-int? "Must be positive"]]
   ["-v" "--version VERSION"
    "What version of Apache Ignite to install"
    :default "2.7.0"]
   ["-o" "--os NAME" "Operating system: either centos or debian."
    :default  :noop
    :parse-fn keyword
    :validate [#{:centos :debian :noop} "One of `centos` or `debian` or 'noop'"]]
   ["-p" "--pds NAME" "Persistence Data Store."
    :default  :false
    :parse-fn str
    :validate [#{"true" "false"} "One of `true` or `false`"]]
   [nil "--url URL" "URL to Ignite zip to install, has precedence over --version"
    :default nil
    :parse-fn str]
   ["-nemesis" "--nemesis Nemesis"
    "What Nemesis to use"
    :default jepsen.nemesis/noop
    :parse-fn nemesis-types
    :validate [identity (jc/one-of nemesis-types)]]])

(defn log-test
  [t]
  (info "Testing\n" (with-out-str (pprint t)))
  t)

(defn test-cmd
  []
  {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
           :opt-fn (fn [parsed]
                     (-> parsed
                         jc/test-opt-fn
                         (jc/rename-options {
                          :test :test-fns})))
           :usage (jc/test-usage)
           :run (fn [{:keys [options]}]
                  (pprint options)
                  (doseq [i        (range (:test-count options))
                          test-fn  (:test-fns options)]
                    ; Rehydrate test and run
                    (let [
                          test (-> options
                                   test-fn
                                   log-test
                                   jepsen/run!)]
                      (when-not (:valid? (:results test))
                        (System/exit 1)))))}})

(defn -main
  [& args]
  (jc/run! (merge (jc/serve-cmd)
                  (test-cmd))
           args))
