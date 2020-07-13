(ns jepsen.independent-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [jepsen [common-test :refer [quiet-logging]]]
            [jepsen.independent :refer :all]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gen.test]))

(use-fixtures :once quiet-logging)

; Tests for independent generators are in generator-test; might want to pull
; them over here later.

(deftest checker-test
  (let [even-checker (reify checker/Checker
                       (check [this test history opts]
                         {:valid? (even? (count history))}))
        history (->> (fn [k] (->> (range k)
                                  (map (partial array-map :value))))
                     (sequential-generator [0 1 2 3])
                     (gen/nemesis nil)
                     (gen.test/perfect (gen.test/n+nemesis-context 3))
                     (concat [{:value :not-sharded}]))]
    (is (= {:valid? false
            :results {1 {:valid? true}
                      2 {:valid? false}
                      3 {:valid? true}}
            :failures [2]}
           (checker/check (checker even-checker)
                          {:name "independent-checker-test"
                           :start-time 0}
                          history
                          {})))))
