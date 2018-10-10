(ns jepsen.faunadb.runner
  "Runs FaunaDB tests. Provides exit status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen [cli :as jc]
                    [core :as jepsen]
                    [web :as web]]
            [jepsen.nemesis.time :as nt]
            [jepsen.fauna :as fauna]
            [jepsen.faunadb [bank :as bank]
                            [register :as register]
                            [sets :as sets]
                            [pages :as pages]
                            [nemesis :as jfn]]))

(def tests
  "A map of test names to test constructors."
  {"sets"       sets/test
   "register"   register/test
   "bank"       bank/test
   "bank-index" bank/index-test
   "pages"      pages/test})

(def nemeses
  "Supported nemeses"
  {"none"              `(jfn/none)
   "parts"             `(jfn/parts)
   "majority-ring"     `(jfn/majring)
   "strobe-skews"      `(jfn/strobe-skews)
   "small-skews"       `(jfn/small-skews)
   "subcritical-skews" `(jfn/subcritical-skews)
   "critical-skews"    `(jfn/critical-skews)
   "big-skews"         `(jfn/big-skews)
   "huge-skews"        `(jfn/huge-skews)
   "start-kill"        `(jfn/startkill 1)
   "start-stop"        `(jfn/startstop 1)})

(def opt-spec
  "Command line options for tools.cli"
  [[nil "--[no-]strong-read" "Force strict reads by performing dummy writes"
    :default true]
   [nil "--at-query" "Use At queries for certain operations, rather than just reading."
    :default false]
   [nil "--fixed-instances" "Don't create and destroy instances dynamically."
    :default false]
   [nil "--serialized-indices" "Use strict serializable indexes"
    :default false]
   [nil "--wait-for-convergence" "Don't start operations until data movement has completed"
    :default false]
   [nil "--datadog-api-key KEY" "If provided, sets up Fauna's integrated datadog stats"]
  (jc/repeated-opt "-t" "--test NAME" "Test(s) to run" [] tests)

  (jc/repeated-opt nil "--nemesis NAME" "Which nemesis to use"
                   [`(jfn/none)]
                   nemeses)

  (jc/repeated-opt nil "--nemesis2 NAME" "An additional nemesis to mix in"
                   [nil]
                   nemeses)

  ])

(defn log-test
  [t]
  (info "Testing\n" (with-out-str (pprint t)))
  t)

(defn nemesis-product
  "The cartesian product of collections of symbols to nemesis functions c1 and
  c2, restricted to remove:

    - Duplicate orders (a,b) (b,a)
    - Pairs of clock-skew nemesis
    - Identical nemesis"
  [c1 c2]
  (->> (for [n1 c1, n2 c2] [n1 n2])
       (reduce (fn [[pairs seen] [n1 n2 :as pair]]
                 (if (or (= n1 n2)
                         (and (:clocks (eval n1)) (:clocks (eval n2)))
                         (seen #{n1 n2}))
                   [pairs seen]
                   [(conj pairs pair) (conj seen #{n1 n2})]))
               [[] #{}])
       first))

(defn test-cmd
  []
  {"test" {:opt-spec (into jc/test-opt-spec opt-spec)
           :opt-fn (fn [parsed]
                     (-> parsed
                         jc/test-opt-fn
                         (jc/rename-options {:nemesis   :nemeses
                                             :nemesis2  :nemeses2
                                             :test      :test-fns})))
           :usage (jc/test-usage)
           :run (fn [{:keys [options]}]
                  (pprint options)
                  (doseq [i        (range (:test-count options))
                          test-fn  (:test-fns options)
                          [n1 n2]  (nemesis-product (:nemeses options)
                                                    (:nemeses2 options))]
                    ; Rehydrate test and run
                    (let [nemesis  (jfn/compose [(eval n1) (eval n2)])
                          test (-> options
                                   (dissoc :test-fns :nemeses :nemeses2)
                                   (assoc :nemesis nemesis)
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
