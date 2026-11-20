(ns jepsen.independent-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [java-time.api :as time]
            [jepsen [common-test :refer [default-start-time quiet-logging]]
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
                           :start-time default-start-time}
                          history
                          {})))))

(defn- recording-gen
  "A generator of writes that conj's, onto the atom seen, the set of threads in
  the context each update hands it."
  [seen]
  (reify gen/Generator
    (op [this test ctx]
      (let [[op _] (gen/op {:f :write, :value 1} test ctx)]
        [op this]))
    (update [this test ctx event]
      (swap! seen conj (set (gen/all-threads ctx)))
      this)))

(deftest concurrent-generator-update-context-test
  ; Updates must pass each key's generator its group's filtered context,
  ; matching op, both before and after the first op call.
  (let [ctx   (gen.test/n+nemesis-context 30)
        event {:type :ok, :process 25, :f :write, :time 0,
               :value (tuple 2 1)}]
    (testing "after op"
      (let [seen  (atom [])
            g     (concurrent-generator 10 (range) (fn [_k] (recording-gen seen)))
            [_ g] (gen/op g {} ctx)]
        (gen/update g {} ctx event)
        (is (= [(set (range 20 30))] @seen))))

    (testing "before op"
      (let [seen (atom [])
            g    (concurrent-generator 10 (range) (fn [_k] (recording-gen seen)))
            g    (gen/update g {} ctx event)]
        (is (= [(set (range 20 30))] @seen))
        (let [[op _] (gen/op g {} ctx)]
          (is (= :write (:f op))))))))
