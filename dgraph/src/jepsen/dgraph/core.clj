(ns jepsen.dgraph.core
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn error]]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.pprint :refer [pprint]]
            [jepsen [cli :as cli]
                    [core :as jepsen]
                    [checker :as checker]
                    [tests :as tests]]
            [jepsen.generator.pure :as gen]
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
                           [trace  :as t]
                           [wr :as wr]]))

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
   :types                     types/workload
   :wr                        wr/workload})

(def standard-workloads
  "The workloads we run for test-all"
  (remove #{:types} (keys workloads)))

(def nemesis-specs
  "These are the types of failures that the nemesis can perform"
  #{:kill-alpha?
    :kill-zero?
    :fix-alpha?
    :partition-halves?
    :partition-ring?
    :move-tablet?
    :skew-clock?})

(defn default-nemesis?
  "Is this nemesis option map, as produced by the CLI, the default?"
  [nemesis-opts]
  (= {} (dissoc nemesis-opts :interval)))

(def standard-nemeses
  "A set of prepackaged nemeses"
  [; Nothing
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
    :kill-zero?       true}])

(def skew-specs
  #{:tiny
    :small
    :big
    :huge})

(defn dgraph-test
  "Builds up a dgraph test map from CLI options."
  [opts]
  (let [version  (if-let [bin (:local-binary opts)]
                   ; We can't pass local filenames directly to sh, or it'll
                   ; look for them in $PATH, not the working directory
                   (let [bin (.getCanonicalPath (io/file bin))
                         v   (:out (sh bin "version"))]
                     (if-let [m (re-find #"Dgraph version   : (v[0-9a-zA-Z\.-]+)" v)]
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
                          {:perf        (checker/perf)
                           :exceptions  (checker/unhandled-exceptions)
                           :stats       (checker/stats)
                           :workload    (:checker workload)})
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
    :default "1.1.1"]
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
   [nil "--dgraph-jaeger-agent AGENT" "Jaeger agent URL to pass to dgraph on startup."]
   ["-f" "--force-download" "Ignore the package cache; download again."
    :default false]
  ["-w" "--workload NAME" "Test workload to run"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]
  ; TODO: rewrite nemesis-interval and nemesis so that we can detect the
  ; absence of these options at the CLI during test-all
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
   [nil "--upsert-schema"
    "If present, tests will use @upsert schema directives. To disable, provide false"
    :parse-fn (complement #{"false"})
    :default true]
   ["-s" "--sequencing MODE" "DEPRECATED: --sequencing flag provided. This will be removed in a future version as server/client sequencing is no longer supported by Dgraph."]
   [nil "--defer-db-teardown" "Wait until user input to tear down DB nodes"
    :default false]])

(defn all-tests
  "Takes base CLI options and constructs a sequence of test options."
  [opts]
  (let [nemeses   (if-let [n (:nemesis opts)]
                    (if (default-nemesis? n) standard-nemeses [n])
                    standard-nemeses)
        workloads (if-let [w (:workload opts)] [w] standard-workloads)
        counts    (range (:test-count opts))
        test-opts (for [i counts, n nemeses, w workloads]
                    (assoc opts
                           :nemesis n
                           :workload w))
        ; Only the first test should force a re-download.
        test-opts (cons (first test-opts)
                        (map #(assoc % :force-download false) (rest test-opts)))]
    ; (pprint test-opts)
    (map dgraph-test test-opts)))

(defn -main
  "Handles command line arguments; running tests or the web server."
  [& args]
  (cli/run! (merge (cli/test-all-cmd {:tests-fn all-tests
                                       :opt-spec cli-opts})
                   (cli/single-test-cmd {:test-fn   dgraph-test
                                         :opt-spec  cli-opts})
                   (cli/serve-cmd))
            args))
