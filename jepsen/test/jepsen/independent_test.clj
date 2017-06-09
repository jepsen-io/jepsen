(ns jepsen.independent-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [jepsen.independent :refer :all]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.generator-test :as gen-test :refer [ops]]))

(deftest sequential-generator-test
  (testing "empty keys"
    (is (= []
           (ops [:a :b] (sequential-generator [] (fn [k] :x))))))

  (testing "one key"
    (is (= [{:value [:k1 :ashley]}
            {:value [:k1 :katchadourian]}]
           (ops [:a] (sequential-generator
                       [:k1]
                       (fn [k] (gen/seq [{:value :ashley}
                                         {:value :katchadourian}])))))))

  (testing "n keys"
    (is (= [[1 0]
            [2 0]
            [2 1]
            [3 0]
            [3 1]
            [3 2]]
           (->> (fn [k] (gen/seq (map (partial array-map :value) (range k))))
                (sequential-generator [1 2 3])
                (ops [:a])
                (map :value)))))

  (testing "concurrency"
    (let [kmax 1000
          vmax 10]
      ; Gotta realize the ranges to work around a concurrency bug in LongRange
      (is (= (set (for [k (range kmax), v (range vmax)] [k v]))
             (->> (fn [k] (gen/seq (map (partial array-map :value)
                                        (doall (range vmax)))))
                  (sequential-generator (doall (range kmax)))
                  (ops (range 10))
                  (map :value)
                  set))))))

(deftest concurrent-generator-test
  (testing "empty keys"
    (is (= []
           (ops (range 10) (concurrent-generator 1 [] identity)))))

  (testing "Too few threads"
    (is (thrown-with-msg?
          Exception
          #"With 10 worker threads, this jepsen\.concurrent/concurrent-generator cannot run a key with 12 threads concurrently\. Consider raising your test's :concurrency to at least 12\."
          (ops (range 10) (concurrent-generator 12 [] identity)))))

  (testing "Uneven threads"
    (is (thrown-with-msg?
          Exception
          #"This jepsen\.independent/concurrent-generator has 11 threads to work with, but can only use 10 of those threads to run 5 concurrent keys with 2 threads apiece\. Consider raising or lowering the test's :concurrency to a multiple of 2\."
          (ops (range 11) (concurrent-generator 2 [] identity)))))

  (testing "Fully concurrent"
    (let [kmax    10
          vmax    5
          n       5
          threads 100]
      (is (= (set (for [k (range kmax), v (range vmax)] [k v]))
             (->> (fn [k] (gen/seq (map (partial array-map :value)
                                        (range vmax))))
                  (concurrent-generator n (range kmax))
                  (ops (range threads))
                  (map :value)
                  set))))))


(deftest checker-test
  (let [even-checker (reify checker/Checker
                       (check [this test model history opts]
                         {:valid? (even? (count history))}))
        history (->> (fn [k] (->> (range k)
                                  (map (partial array-map :value))
                                  gen/seq))
                     (sequential-generator [0 1 2 3])
                     (ops [:a :b :c])
                     (concat [{:value :not-sharded}]))]
    (is (= {:valid? false
            :results {1 {:valid? true}
                      2 {:valid? false}
                      3 {:valid? true}}
            :failures [2]}
           (checker/check (checker even-checker)
                          {:name "independent-checker-test"
                           :start-time 0}
                          nil
                          history
                          {})))))
