(ns jepsen.faunadb.runner
  "Runs FaunaDB tests. Provides exit status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen.cli :as jc]
            [jepsen.core :as jepsen]
            [jepsen.web :as web]
            [jepsen.fauna :as fauna]
            [jepsen.faunadb [bank :as bank]
                            [nemesis :as cln]]))

(def tests
  "A map of test names to test constructors."
  {"bank" bank/test})

(def nemeses
  "Supported nemeses"
  {"none"                       `(cln/none)})

(def opt-spec
  "Command line options for tools.cli"
  [(jc/repeated-opt "-t" "--test NAME" "Test(s) to run" [] tests)

  (jc/repeated-opt nil "--nemesis NAME" "Which nemeses to use"
                   [`(cln/none)]
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
    - Pairs of clock-skew nemeses
    - Identical nemeses"
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
                    (let [nemesis  (cln/compose [(eval n1) (eval n2)])
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
