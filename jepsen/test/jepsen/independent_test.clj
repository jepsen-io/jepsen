(ns jepsen.independent-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [jepsen [common-test :refer [quiet-logging]]
                    [history :as h]]
            [jepsen.independent :refer :all]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.generator.test :as gen.test]
            [jepsen.history.core :as hc :refer [chunked]]))

(use-fixtures :once quiet-logging)

; Tests for independent generators are in generator-test; might want to pull
; them over here later.

(deftest subhistories-test
  (let [n 12
        h0 (->> (range n)
               (mapv (fn [i]
                       {:type :invoke, :f :foo, :value (tuple (mod i 3) i)})))
        ; We want to explicitly chunk this history
        chunk-size 3
        chunk-count (/ n chunk-size)
        _ (assert integer? chunk-count)
        h (h/history
            (hc/soft-chunked-vector
              chunk-count
              ; Starting indices
              (range 0 n chunk-size)
              ; Loader
              (fn load-nth [i]
                (let [start (* chunk-size i)]
                  (subvec h0 start (+ start chunk-size))))))
        shs (subhistories (history-keys h) h)]
    (is (= {0 [0 3 6 9]
            1 [1 4 7 10]
            2 [2 5 8 11]}
           (update-vals shs (partial map :value))))))

(deftest checker-test
  (let [even-checker (reify checker/Checker
                       (check [this test history opts]
                         {:valid? (even? (count history))}))
        history (->> (fn [k] (->> (range k)
                                  (map (partial array-map :value))))
                     (sequential-generator [0 1 2 3])
                     (gen/nemesis nil)
                     (gen.test/perfect (gen.test/n+nemesis-context 3))
                     (concat [{:value :not-sharded}])
                     (h/history))]
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
