(ns aerospike.core
  "Entry point for aerospike tests"
  (:require [aerospike [support :as support]
             [counter :as counter]
             [cas-register :as cas-register]
             [nemesis :as nemesis]
             [set :as set]]
            [clojure.tools.logging :refer [info]]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as string]
            [jepsen [cli :as cli]
             [control :as c]
             [checker :as checker]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.os.debian :as debian])
  (:gen-class))

(def txns-enabled
  (string/starts-with? (System/getenv "JAVA_CLIENT_REF") "CLIENT-2848"))

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
   (let [res {:cas-register (cas-register/workload)
              :counter      (counter/workload)
              :set          (set/workload)}]
     (if txns-enabled
       (do (require '[aerospike.transact :as transact]) ; for alias only(?)
          ;; add MRT workloads iff client branch supports it
           (assoc res
                  :transact ((requiring-resolve 'transact/workload) opts)
                  :list-append ((requiring-resolve 'transact/workload-ListAppend) opts)))
       ;; otherwise, return SRT workloads only
       res))))

(defn workload+nemesis
  "Finds the workload and nemesis for a given set of parsed CLI options."
  [opts]
  (info "In Call to (workload+nemesis) with opts: " opts)
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
        time-limit (:time-limit opts)
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
    (info "constructed jepsen test-map from opts:=" opts "\nWith CLIENT:=" client)
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
    :default 2
    :parse-fn #(Long/parseLong %)
                 ; TODO: must be >= min-txn-length
    :validate [pos? "must be positive"]]
   [nil "--min-txn-length MIN" "Maximum number of micro-ops per transaction"
    :default 2
    :parse-fn #(Long/parseLong %)
                 ; TODO: must be <= min-txn-length
    :validate [pos? "must be positive"]]
   [nil "--key-count N_KEYS" "Number of active keys at any given time"
    :default  3 ; TODO: make this  default differently based on key-dist 
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]
   [nil "--max-writes-per-key N_WRITES" "Limit of writes to a particular key"
    :default  32 ; TODO: make this  default differently based on key-dist 
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
   [nil "--clean-kill" "Terminate processes with SIGTERM to simulate fsync before commit"
    :default false]
   [nil "--no-revives" "Don't revive during the test (but revive at the end)"
    :default false]
   [nil "--no-clocks" "Allow the nemesis to change the clock"
    :default  false
    :assoc-fn (fn [m k v] (assoc m :no-clocks v))]
   [nil "--no-partitions" "Allow the nemesis to introduce partitions"
    :default  false
    :assoc-fn (fn [m k v] (assoc m :no-partitions v))]
   [nil "--nemesis-interval SECONDS" "How long between nemesis actions?"
    :default 5
    :parse-fn #(Long/parseLong %)
    :validate [(complement neg?) "Must be non-negative"]]
   [nil "--no-kills" "Allow the nemesis to kill processes."
    :default  false
    :assoc-fn (fn [m k v] (assoc m :no-kills v))]
   [nil "--pause-mode MODE" "Whether to pause nodes by pausing the process, or slowing the network"
    :default :process
    :parse-fn keyword
    :validate [#{:process :net :clock} "Must be one of :clock, :process, :net."]]
   [nil "--heartbeat-interval MS" "Aerospike heartbeat interval in milliseconds"
    :default 150
    :parse-fn #(Long/parseLong %)
    :validate [pos? "must be positive"]]])

(def opt-spec
  (if txns-enabled
    (cli/merge-opt-specs srt-opt-spec mrt-opt-spec)
    srt-opt-spec))


(defn valid-opts
  "returns a map with valid choices for each test option."
  [opts]
  (let [txn-min-ops        (rand-nth (range 1 6))
        key-dist           (rand-nth (list :exponential :uniform))
        nKeys              (if (= key-dist :uniform)
                             (rand-nth (range 3 8))
                             (rand-nth (range 8 12)))
        cluster-size       (count (:nodes opts))
        nClients           (if (= (:workload opts) :set)
                             (rand-nth (list 10 30 60))
                             (rand-nth (distinct (list
                                                  1 2 3 5 8 10 30
                                                  cluster-size
                                                  (* cluster-size 2)
                                                  (* cluster-size 3)))))]
    {:concurrency          nClients
     ; Txn Specific
     :min-txn-length       txn-min-ops
     :max-txn-length       (rand-nth (range txn-min-ops 12))
     :key-count            nKeys
     :key-dist             key-dist
     ; Nemesis related
     :no-kills             (rand-nth (list true false))
     :clean-kills          (rand-nth (list true false))
     :no-partitions        (rand-nth (list true false))
     :no-clocks            (rand-nth (list true false))
     :no-revives           (rand-nth (list true false))
     :nemesis-interval     (rand-nth (list 5 8 10 15 20))}))

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
  (info "making option map for (all-tests)'s call to `aerospike-test` with options\n"
        (with-out-str (pprint opts)))
  (let [node-opt (:nodes opts)
        nClients (:concurrency opts)
        duration (:time-limit opts)
        ssh-opts (:ssh opts)
        ;; _ (info "OPT-WRKLD:" (get (workloads opts) (:workload opts)))
        base-opts (valid-opts opts)]
    (info "VALIDATED OPTION MAP: " base-opts)
    (list
     (merge base-opts opts {:workload :set})
     (merge base-opts opts {:workload :counter})
     (merge base-opts opts {:workload :cas-register})
     (merge base-opts opts {:workload :list-append})
     (merge base-opts opts {:workload :transact}))))

(defn all-tests
  "
  Takes base CLI options and constructs a sequence of test option maps to be used with test-all-cmd.
  "
  [opts]
  ;; (info "Constructing -all-tests- from map! options:" (with-out-str (pprint opts)))
  (let [res (map aerospike-test (all-test-opts opts))]
    ;; (info "Returning from (all-tests)") 
    ;; (info (:workload (first res)))
    res))

(defn -main
  "Handles command-line arguments, running a Jepsen command."
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd
           {:test-fn   aerospike-test
            :opt-spec  opt-spec})
          (cli/test-all-cmd
           {:tests-fn  all-tests
            :opt-spec  opt-spec}))
   args))