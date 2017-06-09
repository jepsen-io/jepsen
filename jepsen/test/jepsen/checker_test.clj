(ns jepsen.checker-test
  (:refer-clojure :exclude [set])
  (:use jepsen.checker
        clojure.test)
  (:require [knossos.core :refer [ok-op invoke-op]]
            [multiset.core :as multiset]
            [jepsen.model :as model]
            [jepsen.checker.perf :refer :all]))

(deftest queue-test
  (testing "empty"
    (is (:valid? (check (queue) nil nil [] {}))))

  (testing "Possible enqueue but no dequeue"
    (is (:valid? (check (queue) nil (model/unordered-queue)
                        [(invoke-op 1 :enqueue 1)] {}))))

  (testing "Definite enqueue but no dequeue"
    (is (:valid? (check (queue) nil (model/unordered-queue)
                        [(ok-op 1 :enqueue 1)] {}))))

  (testing "concurrent enqueue/dequeue"
    (is (:valid? (check (queue) nil (model/unordered-queue)
                        [(invoke-op 2 :dequeue nil)
                         (invoke-op 1 :enqueue 1)
                         (ok-op     2 :dequeue 1)] {}))))

  (testing "dequeue but no enqueue"
    (is (not (:valid? (check (queue) nil (model/unordered-queue)
                             [(ok-op 1 :dequeue 1)] {}))))))

(deftest total-queue-test
  (testing "empty"
    (is (:valid? (check (total-queue) nil nil [] {}))))

  (testing "sane"
    (is (= (check (total-queue) nil nil
                  [(invoke-op 1 :enqueue 1)
                   (invoke-op 2 :enqueue 2)
                   (ok-op     2 :enqueue 2)
                   (invoke-op 3 :dequeue 1)
                   (ok-op     3 :dequeue 1)
                   (invoke-op 3 :dequeue 2)
                   (ok-op     3 :dequeue 2)]
                  {})
           {:valid?           true
            :duplicated       (multiset/multiset)
            :lost             (multiset/multiset)
            :unexpected       (multiset/multiset)
            :recovered        (multiset/multiset 1)
            :ok-frac          1
            :unexpected-frac  0
            :lost-frac        0
            :duplicated-frac  0
            :recovered-frac   1/2})))

  (testing "pathological"
    (is (= (check (total-queue) nil nil
                  [(invoke-op 1 :enqueue :hung)
                   (invoke-op 2 :enqueue :enqueued)
                   (ok-op     2 :enqueue :enqueued)
                   (invoke-op 3 :enqueue :dup)
                   (ok-op     3 :enqueue :dup)
                   (invoke-op 4 :dequeue nil) ; nope
                   (invoke-op 5 :dequeue nil)
                   (ok-op     5 :dequeue :wtf)
                   (invoke-op 6 :dequeue nil)
                   (ok-op     6 :dequeue :dup)
                   (invoke-op 7 :dequeue nil)
                   (ok-op     7 :dequeue :dup)]
                  {})
           {:valid?           false
            :lost             (multiset/multiset :enqueued)
            :unexpected       (multiset/multiset :wtf)
            :recovered        (multiset/multiset)
            :duplicated       (multiset/multiset :dup)
            :ok-frac          1/3
            :lost-frac        1/3
            :unexpected-frac  1/3
            :duplicated-frac  1/3
            :recovered-frac   0}))))

(deftest counter-test
  (testing "empty"
    (is (= (check (counter) nil nil [] {})
           {:valid? true
            :reads  []
            :errors []})))

  (testing "initial read"
    (is (= (check (counter) nil nil
                  [(invoke-op 0 :read nil)
                   (ok-op     0 :read 0)]
                  {})
           {:valid? true
            :reads  [[0 0 0]]
            :errors []})))

  (testing "initial invalid read"
    (is (= (check (counter) nil nil
                  [(invoke-op 0 :read nil)
                   (ok-op     0 :read 1)]
                  {})
           {:valid? false
            :reads  [[0 1 0]]
            :errors [[0 1 0]]})))

  (testing "interleaved concurrent reads and writes"
    (is (= (check (counter) nil nil
                  [(invoke-op 0 :read nil)
                   (invoke-op 1 :add 1)
                   (invoke-op 2 :read nil)
                   (invoke-op 3 :add 2)
                   (invoke-op 4 :read nil)
                   (invoke-op 5 :add 4)
                   (invoke-op 6 :read nil)
                   (invoke-op 7 :add 8)
                   (invoke-op 8 :read nil)
                   (ok-op     0 :read 6)
                   (ok-op     1 :add 1)
                   (ok-op     2 :read 0)
                   (ok-op     3 :add 2)
                   (ok-op     4 :read 3)
                   (ok-op     5 :add 4)
                   (ok-op     6 :read 100)
                   (ok-op     7 :add 8)
                   (ok-op     8 :read 15)]
                  {})
           {:valid? false
            :reads  [[0 6 15] [0 0 15] [0 3 15] [0 100 15] [0 15 15]]
            :errors [[0 100 15]]})))

  (testing "rolling reads and writes"
    (is (= (check (counter) nil nil
                  [(invoke-op 0 :read nil)
                   (invoke-op 1 :add  1)
                   (ok-op     0 :read 0)
                   (invoke-op 0 :read nil)
                   (ok-op     1 :add  1)
                   (invoke-op 1 :add  2)
                   (ok-op     0 :read 3)
                   (invoke-op 0 :read nil)
                   (ok-op     1 :add  2)
                   (ok-op     0 :read 5)] {})
           {:valid? false
            :reads  [[0 0 1] [0 3 3] [1 5 3]]
            :errors [[1 5 3]]}))))

(deftest compose-test
  (is (= (check (compose {:a (unbridled-optimism) :b (unbridled-optimism)})
                nil nil nil {})
         {:a {:valid? true}
          :b {:valid? true}
          :valid? true})))

(deftest bucket-points-test
  (is (= (bucket-points 2
                        [[1 :a]
                         [7 :g]
                         [5 :e]
                         [2 :b]
                         [3 :c]
                         [4 :d]
                         [6 :f]])
         {1 [[1 :a]]
          3 [[2 :b]
             [3 :c]]
          5 [[5 :e]
             [4 :d]]
          7 [[7 :g]
             [6 :f]]})))

(deftest latencies->quantiles-test
  (is (= {0 [[5/2 0]  [15/2 20] [25/2 25]]
          1 [[5/2 10] [15/2 25] [25/2 25]]}
         (latencies->quantiles 5 [0 1] (partition 2 [0 0
                                                     1 10
                                                     2 1
                                                     3 1
                                                     4 1
                                                     5 20
                                                     6 21
                                                     7 22
                                                     8 25
                                                     9 25
                                                     10 25])))))

(deftest perf-test
  (check (perf)
         {:name       "perf test"
          :start-time 0}
         nil
         (->> (repeatedly #(/ 1e9 (inc (rand-int 1000))))
              (mapcat (fn [latency]
                        (let [f (rand-nth [:write :read])
                              proc (rand-int 100)
                              time (* 1e9 (rand-int 100))
                              type (rand-nth [:ok :ok :ok :ok :ok
                                              :fail :info :info])]
                          [{:process proc, :type :invoke, :f f, :time time}
                           {:process proc, :type type,    :f f, :time 
                            (+ time latency)}])))
              (take 10000)
              vec)
         {}))
