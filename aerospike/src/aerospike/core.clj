(ns aerospike.core
  "Entry point for aerospike tests"
  (:require [aerospike [support :as support]
             [counter :as counter]
             [cas-register :as cas-register]
             [nemesis :as nemesis]
             [set :as set]
             [transact :as transact]]
            [jepsen [cli :as cli]
             [checker :as checker]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.os.debian :as debian])
  (:gen-class))

;; (defn txns-enabled
;;   (string/starts-with? (System/getenv "JAVA_CLIENT_REF") "CLIENT-2848"))

(defn workloads
  "The workloads we can run. Each workload is a map like

      {:generator         a generator of client ops
       :final-generator   a generator to run after the cluster recovers
       :client            a client to execute those ops
       :checker           a checker
       :model             for the checker}

  Or, for some special cases where nemeses and workloads are coupled, we return
  a keyword here instead."
  ([]
   (workloads {}))
  ([opts]
   {:cas-register (cas-register/workload)
    :counter      (counter/workload)
    :set          (set/workload)
    :transact     (transact/workload opts)
    :list-append  (transact/workload-ListAppend opts)
    }))

(defn workload+nemesis
  "Finds the workload and nemesis for a given set of parsed CLI options."
  [opts]
  (case (:workload opts)
    {:workload (get (workloads opts) (:workload opts))
     :nemesis  (nemesis/full opts)}))

(defn aerospike-test
  "Constructs a Jepsen test map from CLI options."
  [opts]
  (let [{:keys [workload nemesis]} (workload+nemesis opts)
        {:keys [generator
                final-generator
                client
                checker
                model]} workload
        generator (->> generator
                       (gen/nemesis
                        (->> (:generator nemesis)
                             (gen/delay (if (= :pause (:workload opts))
                                          0 ; The pause workload has its own
                                             ; schedule
                                          (:nemesis-interval opts)))))
                       (gen/time-limit (:time-limit opts)))
        generator (if-not (or final-generator (:final-generator nemesis))
                    generator
                    (gen/phases generator
                                (gen/log "Healing cluster")
                                (gen/nemesis (:final-generator nemesis))
                                (gen/log "Waiting for quiescence")
                                (gen/sleep 10)
                                (gen/clients final-generator)))]
    (merge tests/noop-test
           opts
           {:name     (str "aerospike " (name (:workload opts)))
            :os       debian/os
            :db       (support/db opts)
            :client   client
            :nemesis  (:nemesis nemesis)
            :generator generator
            :checker  (checker/compose
                       {:perf (checker/perf)
                        :workload checker})
            :model    model})))

(def mrt-opt-spec "Options for Elle-based workloads"
  [[nil "--max-txn-length MAX" "Maximum number of micro-ops per transaction"
    :parse-fn #(Long/parseLong %)
                 ; TODO: must be >= min-txn-length
    :validate [pos? "must be positive"]]
   [nil "--min-txn-length MIN" "Maximum number of micro-ops per transaction"
    :parse-fn #(Long/parseLong %)
                 ; TODO: must be <= min-txn-length
    :validate [pos? "must be positive"]]
   [nil "--key-count N_KEYS" "Number of active keys at any given time"
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]
   [nil "--max-writes-per-key N_WRITES" "Limit of writes to a particular key"
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]])

(def srt-opt-spec
  "Additional command-line options"
  [[nil "--workload WORKLOAD" "Test workload to run"
    :parse-fn keyword
    :default :all
    :missing (str "--workload " (cli/one-of (workloads)))
    :validate [(assoc (workloads) :all "all") (cli/one-of (assoc (workloads) :all "all"))]]
   [nil "--max-dead-nodes NUMBER" "Number of nodes that can simultaneously fail"
    :parse-fn #(Long/parseLong %)
    :default  2
    :validate [(complement neg?) "must be non-negative"]]
   [nil "--clean-kill" "Terminate processes with SIGTERM to simulate fsync before commit"]
   [nil "--no-revives" "Don't revive during the test (but revive at the end)"]
   [nil "--no-clocks" "Allow the nemesis to change the clock"
    :assoc-fn (fn [m k v] (assoc m :no-clocks v))]
   [nil "--no-partitions" "Allow the nemesis to introduce partitions"
    :assoc-fn (fn [m k v] (assoc m :no-partitions v))]
   [nil "--nemesis-interval SECONDS" "How long between nemesis actions?"
    :parse-fn #(Long/parseLong %)
    :validate [(complement neg?) "Must be non-negative"]]
   [nil "--no-kills" "Allow the nemesis to kill processes."
    :assoc-fn (fn [m k v] (assoc m :no-kills v))]])

(def opt-spec
  (cli/merge-opt-specs srt-opt-spec mrt-opt-spec))


(defn valid-opts
  "returns a map with randomized valid choices for missing test options."
  [opts]
  (let [; Only use command-line provided concurrency, ignore default (1n)
        nClients           (if (.contains (:argv opts) "--concurrency")
                             (:concurrency opts)
                             (* 2 (+ 1 (rand-int 15))))]
    (merge opts
           {:concurrency          nClients
            ; Nemesis related
            :no-kills             (:no-kills opts (rand-nth (list true false))) 
            :clean-kill           (:clean-kill opts (rand-nth (list true false)))
            :no-partitions        (:no-partitions opts (rand-nth (list true false))) 
            :no-clocks            (:no-clocks opts (rand-nth (list true false))) 
            :no-revives           (:no-revives opts (rand-nth (list true false))) 
            :nemesis-interval     (:nemesis-interval opts (rand-nth (list 5 8 10 15 20))) })))

;; -- Why did we merge in the webserver before?
;; (defn -main
;;   "Handles command-line arguments, running a Jepsen command."
;;   [& args]
;;   (cli/run! (merge (cli/single-test-cmd {:test-fn   aerospike-test
;;                                          :opt-spec  opt-spec})
;;                    (cli/serve-cmd))
;;             args))

(defn all-test-opts
  "Creates a list of valid maps for each to be passed to `aerospike-test`"
  [opts]
  (list
   (merge (valid-opts opts) {:workload :set})
   (merge (valid-opts opts) {:workload :counter})
   (merge (valid-opts opts) {:workload :cas-register})
   (merge (valid-opts opts) {:workload :list-append})
   (merge (valid-opts opts) {:workload :transact})))

(defn all-tests
    "Takes base CLI options and constructs a sequence of test option maps
     to be used with test-all-cmd."
    [opts]
    (map aerospike-test (all-test-opts opts)))

(defn single-test [opts]
  (aerospike-test (valid-opts opts)))

(defn -main
    "Handles command-line arguments, running a Jepsen command."
    [& args]
    (cli/run!
     (merge (cli/single-test-cmd
             {:test-fn   single-test
              :opt-spec  opt-spec})
            (cli/test-all-cmd
             {:tests-fn  all-tests
              :opt-spec  opt-spec}))
     args))