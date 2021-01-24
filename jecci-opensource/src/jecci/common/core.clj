(ns jecci.common.core
  "Runs Jecci tests. Provides exit status reporting."
  (:gen-class)
  (:refer-clojure :exclude [test])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen [cli :as jc]
                    [checker :as checker]
                    [core :as jepsen]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.os.debian :as debian]
            [jecci.common [bank :as bank]
                  [db :as db]
                  [long-fork :as long-fork]
                  [monotonic :as monotonic]
                  [nemesis :as nemesis]
                  [register :as register]
                  [sequential :as sequential]
                  [sets :as set]
                  [table :as table]]
            [jecci.interface.nemesis :as in]
            [jecci.utils.colors :as juc]))

(def oses
  "Supported operating systems"
  {"debian" debian/os
   "none"   os/noop})

(def workloads
  "A map of workload names to functions that can take CLI opts and construct
  workloads."
  {:bank            bank/workload
   :bank-multitable bank/multitable-workload
   :long-fork       long-fork/workload
   :monotonic       monotonic/inc-workload
   :txn-cycle       monotonic/txn-workload
   :append          monotonic/append-workload
   :register        register/workload
   :set             set/workload
   :set-cas         set/cas-workload
   :sequential      sequential/workload
   :table           table/workload})

(def workload-options
  "For each workload, a map of workload options to all values that option
  supports."
  {:append          {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :read-lock         [nil "FOR UPDATE"]
                     :predicate-read    [true false]}
   :bank            {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :update-in-place   [true false]
                     :read-lock         [nil "FOR UPDATE"]}
   :bank-multitable {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :update-in-place   [true false]
                     :read-lock         [nil "FOR UPDATE"]}
   :long-fork       {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :use-index         [true false]}
   :monotonic       {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :use-index         [true false]}
   :register        {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :read-lock         [nil "FOR UPDATE"]
                     :use-index         [true false]}
   :set             {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]}
   :set-cas         {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]
                     :read-lock         [nil "FOR UPDATE"]}
   :sequential      {:auto-retry        [true false]
                     :auto-retry-limit  [10 0]}
   :table           {}})

(def workload-options-expected-to-pass
  "Workload options restricted to only those we expect to pass."
  (-> (util/map-vals #(assoc %
                             :auto-retry        [false]
                             :auto-retry-limit  [0])
                     workload-options)))

(def quick-workload-options
  "A restricted set of workload options which skips some redundant tests and
  avoids testing auto-retry or read locks."
  (-> (util/map-vals (fn [opts]
                       (let [opts (-> opts
                                      (assoc
                                        :auto-retry        [:default]
                                        :auto-retry-limit  [:default]
                                        :update-in-place   [false]
                                        :read-lock         [nil])
                                      (update :use-index
                                              (partial filter true?)))]
                         ; Don't generate an empty use-index option
                         (if (seq (:use-index opts))
                           opts
                           (dissoc opts :use-index))))
                     workload-options)
      ; Bank-multitable is, I think, more likely to fail than bank, and the two
      ; test the same invariants in similar ways. Long-fork, monotonic, and
      ; seqwuential are covered by append (though less efficiently, I suspect).
      ; :table isn't as high-priority a correctness check, since it applies
      ; only to DDL.
      (dissoc :bank :long-fork :monotonic :sequential :table)))

(defn all-combos
  "Takes a map of options to collections of values for that option. Computes a
  collection of maps with the combinatorial expansion of every possible option
  value."
  ([opts]
   (all-combos {} opts))
  ([m opts]
   (if (seq opts)
     (let [[k vs] (first opts)]
       (mapcat (fn [v]
                 (all-combos (assoc m k v) (next opts)))
               vs))
     (list m))))

(defn all-workload-options
  "Expands workload-options into all possible CLI opts for each combination of
  workload options."
  [workload-options]
  (mapcat (fn [[workload opts]]
            (all-combos {:workload workload} opts))
          workload-options))

(def nemeses-plot-spec
  (let [default-spec {:nemeses #{{:name        "partition"
                                  :color       (juc/ret-rm-color)
                                  :start       #{:start-partition}
                                  :stop        #{:stop-partition}}}}]
    (assoc default-spec :nemeses
           (into (:nemeses default-spec) (:nemeses in/plot-spec)))))

(defn test
  "Constructs a test from a map of CLI options."
  [opts]
  (let [name (str (:system opts) " " (:version opts)
                  " " (name (:workload opts))
                  (when (:auto-retry opts)
                    " auto-retry ")
                  (when (not= 0 (:auto-retry-limit opts))
                    (str " auto-retry-limit " (:auto-retry-limit opts)))
                  (when (:update-in-place opts)
                    " update-in-place")
                  (when (:read-lock opts)
                    (str " select " (:read-lock opts)))
                  (when (:predicate-read opts)
                    " predicate-read")
                  (when (:use-index opts)
                     " use-index")
                  (when-not (= [:interval] (keys (:nemesis opts)))
                    (str " nemesis " (->> (dissoc (:nemesis opts)
                                                   :interval
                                                   :schedule
                                                   :long-recovery)
                                          keys
                                          (map name)
                                          sort
                                          (str/join ",")))))
        workload  ((get workloads (:workload opts)) opts)
        nemesis   (nemesis/nemesis opts)
        gen       (->> (:generator workload)
                       (gen/nemesis (:generator nemesis))
                       (gen/time-limit (:time-limit opts)))
        gen       (if (:final-generator workload)
                    (gen/phases gen
                                (gen/log "Healing cluster")
                                (gen/nemesis (:final-generator nemesis))
                                (gen/log "Waiting for recovery")
                                (gen/sleep (:final-recovery-time opts))
                                (gen/clients (:final-generator workload)))
                    gen)]
    (merge tests/noop-test
           opts
           (dissoc workload :final-generator)
           {:name       name
            :os         debian/os
            :db         (db/db)
            :client     (:client workload)
            :nemesis    (:nemesis nemesis)
            :generator  gen
            :plot       nemeses-plot-spec
            :checker    (checker/compose
                          {:perf        (checker/perf)
                           :clock-skew  (checker/clock-plot)
                           :workload    (:checker workload)})})))

(def nemesis-specs
  (into #{; partition
            :partition-one
            :partition-half
            :partition-ring}
          in/nemesis-specs))

(defn parse-nemesis-spec
  "Parses a comma-separated string of nemesis types, and turns it into an
  option map like {:kill-alpha? true ...}"
  [s]
  (if (= s "none")
    {}
    (->> (str/split s #",")
         (map (fn [o] [(keyword o) true]))
         (into {}))))

(defn parse-long [x] (Long/parseLong x))

(def pure-gen-opts
  "traditional generators is replaced by pure generators after 2.0"
  [
   [nil "--pure-generators"]
   ])

(def cli-opts
  "Command line options for tools.cli"
  [
   ;(jc/repeated-opt "-p" "--port PORT" "Port(s) to ssh to. Flag may be submitted many times, with one port per flag." [22 22 22 22 22])
   [nil "--port PORT" "port for ssh"
    :parse-fn parse-long
    :default 22]

   [nil "--faketime MAX_RATIO"
    "Use faketime to skew clock rates up to MAX_RATIO"
    :parse-fn #(Double/parseDouble %)
    :validate [pos? "should be a positive number"]]

    [nil "--force-reinstall" "Don't re-use an existing installed directory"]

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
                      (every? nemesis-specs (keys parsed))))
               (str "Should be a comma-separated list of failure types. A failure "
                    (.toLowerCase (jc/one-of nemesis-specs))
                    ". Or, you can use 'none' to indicate no failures.")]]

   [nil "--nemesis-long-recovery" "Every so often, have a long period of no faults, to see whether the cluster recovers."
    :default false
    :assoc-fn (fn [m k v] (update m :nemesis assoc :long-recovery v))]

   [nil "--nemesis-schedule SCHEDULE" "Whether to have randomized delays between nemesis actions, or fixed ones."
    :parse-fn keyword
    :assoc-fn (fn [m k v] (update m :nemesis assoc :schedule v))
    :validate [#{:fixed :random} "Must be either 'fixed' or 'random'"]]

   ["-o" "--os NAME" "debian, or none"
    :default debian/os
    :parse-fn oses
    :validate [identity (jc/one-of oses)]]

   [nil "--recovery-time SECONDS"
    "How long to wait for cluster recovery before final ops."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--tarball-url URL" "URL to your cool system tarball to install, has precedence over --version"
    :default nil]])

(def test-all-opts
  "CLI options for running the entire test suite."
  [[nil "--quick" "Runs a limited set of workloads and nemeses for faster testing."
    :default false]

   [nil "--very-quick" "Runs a limited set of workloads and nemeses for faster testing."
        :default false]

   ["-w" "--workload NAME"
    "Test workload to run. If omitted, runs all workloads"
    :parse-fn keyword
    :default nil
    :validate [workloads (jc/one-of workloads)]]

   [nil "--only-workloads-expected-to-pass"
    "If present, skips tests which are not expected to pass, given Fauna's docs"
    :default false]])

(def single-test-opts
  "CLI options for running a single test"

   [[nil "--predicate-read" "If present, try to read using a query over a secondary key, rather than by primary key. Implied by --use-index."
    :default false]

   [nil "--read-lock TYPE"
    "What kind of read locks, if any, should we acquire? Default is none; may
    also be 'update'."
    :default nil
    :parse-fn {"update" "FOR UPDATE"}
    :validate [#{nil "FOR UPDATE"} "Should be FOR UPDATE"]]

   [nil "--update-in-place"
    "If true, performs updates (on some workloads) in place, rather than
    separating read and write operations."
    :default false]

   ["-i" "--use-index" "Whether to use indices, or read by primary key"
    :default false]

   ["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :missing (str "--workload " (jc/one-of workloads))
    :validate [workloads (jc/one-of workloads)]]])

(defn rename-port
  "take params in argvs and put it into ssh options"
  [parsed]
  (let [{:keys [port]} (:options parsed)]
    (assoc parsed :options
                  (-> (:options parsed)
                    (assoc :ssh (-> (:ssh (:options parsed))
                                  (assoc :port port)))
                    (dissoc :port)))))

(defn throw-on-nil [e & [msg]]
  (when (nil? e)
    (throw (Exception. msg)))
  e)

(defn test-all-cmd
  "A command that runs a whole suite of tests in one go."
  [opt-spec]
  {"test-all"
   {:opt-spec opt-spec
    :opt-fn   jc/test-opt-fn
    :usage    "Runs all combinations of workloads, nemeses, and options."
    :run      (fn [{:keys [options]}]
                (info "CLI options:\n" (with-out-str (pprint options)))
                (let [w         (:workload options)
                      workload-opts (cond
                                      (:quick options)
                                      quick-workload-options

                                      (:very-quick options)
                                      quick-workload-options

                                      (:only-workloads-expected-to-pass options)
                                      workload-options-expected-to-pass

                                      true
                                      workload-options)
                      workloads (cond->> (all-workload-options workload-opts)
                                  w (filter (comp #{w} :workload)))
                      nemeses   (let []
                                  (cond
                                    (:quick options)  (throw-on-nil in/quick-nemeses 
                                                                    "No quick-nemeses implemented")
                                    (:very-quick options) (throw-on-nil in/very-quick-nemeses
                                                                    "No very-quick-nemeses implemented")
                                    true              (throw-on-nil in/all-nemeses
                                                                    "No all-nemeses implemented")))
                      tests (for [nemesis   nemeses
                                  workload  workloads
                                  i         (range (:test-count options))]
                              (do
                              (-> options
                                  (merge workload)
                                  (update :nemesis merge nemesis))))
                      failed-tests []]
                  (->> tests
                       (map-indexed
                         (fn [i test-opts]
                           (try
                             (info "\n\n\nTest " (inc i) "/" (count tests))
                             (jepsen/run! (test test-opts))
                             (catch Exception e
                               (warn e "Test crashed; moving on...")
                               (update failed-tests (count failed-tests) test-opts)))))
                       dorun)
                  (for [opts failed-tests]
                    (error opts))))}})

(defmacro prn-all-tr-wrapper
  [& body]
  `(try ~@body
       ~'(catch Exception e
         (info "caught exception: " (.getMessage e))
         (->>
           (Thread/getAllStackTraces)
           (map (fn [[thread frames]] [(.getName thread) (mapv #(read-string (pr-str %)) frames)]))
           (into {})
           (info)))))

(defn -main
  [& args]
  (jc/run!
    (merge  (jc/serve-cmd)
            (test-all-cmd (concat jc/test-opt-spec cli-opts test-all-opts pure-gen-opts))
            (prn-all-tr-wrapper
              (jc/single-test-cmd {:test-fn  test
                                   :opt-spec (concat cli-opts single-test-opts pure-gen-opts)
                                   :opt-fn rename-port}))
            )
    args))
