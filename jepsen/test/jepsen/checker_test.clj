(ns jepsen.checker-test
  (:refer-clojure :exclude [set])
  (:use jepsen.checker
        clojure.test)
  (:require [knossos.core :refer [ok-op invoke-op]]
            [multiset.core :as multiset]
            [jepsen.model :as model]))

(deftest queue-test
  (testing "empty"
    (is (:valid? (check queue nil nil []))))

  (testing "Possible enqueue but no dequeue"
    (is (:valid? (check queue nil (model/unordered-queue)
                        [(invoke-op 1 :enqueue 1)]))))

  (testing "Definite enqueue but no dequeue"
    (is (:valid? (check queue nil (model/unordered-queue)
                        [(ok-op 1 :enqueue 1)]))))

  (testing "concurrent enqueue/dequeue"
    (is (:valid? (check queue nil (model/unordered-queue)
                        [(invoke-op 2 :dequeue nil)
                         (invoke-op 1 :enqueue 1)
                         (ok-op     2 :dequeue 1)]))))

  (testing "dequeue but no enqueue"
    (is (not (:valid? (check queue nil (model/unordered-queue)
                             [(ok-op 1 :dequeue 1)]))))))

(deftest total-queue-test
  (testing "empty"
    (is (:valid? (check total-queue nil nil []))))

  (testing "sane"
    (is (= (check total-queue nil nil
                  [(invoke-op 1 :enqueue 1)
                   (invoke-op 2 :enqueue 2)
                   (ok-op     2 :enqueue 2)
                   (invoke-op 3 :dequeue 1)
                   (ok-op     3 :dequeue 1)
                   (invoke-op 3 :dequeue 2)
                   (ok-op     3 :dequeue 2)])
           {:valid?           true
            :lost             (multiset/multiset)
            :unexpected       (multiset/multiset)
            :recovered        (multiset/multiset 1)
            :ok-frac          1
            :unexpected-frac  0
            :lost-frac        0
            :recovered-frac   1/2})))

  (testing "pathological"
    (is (= (check total-queue nil nil
                  [(invoke-op 1 :enqueue :hung)
                   (invoke-op 2 :enqueue :enqueued)
                   (ok-op     2 :enqueue :enqueued)
                   (invoke-op 3 :dequeue nil) ; nope
                   (invoke-op 4 :dequeue nil)
                   (ok-op     4 :dequeue :wtf)])
           {:valid?           false
            :lost             (multiset/multiset :enqueued)
            :unexpected       (multiset/multiset :wtf)
            :recovered        (multiset/multiset)
            :ok-frac          0
            :lost-frac        1/2
            :unexpected-frac  1/2
            :recovered-frac   0}))))

(deftest counter-test
  (testing "empty"
    (is (= (check counter nil nil [])
           {:valid? true
            :reads  []
            :errors []})))

  (testing "initial read"
    (is (= (check counter nil nil
                  [(invoke-op 0 :read nil)
                   (ok-op     0 :read 0)])
           {:valid? true
            :reads  [[0 0 0]]
            :errors []})))

  (testing "initial invalid read"
    (is (= (check counter nil nil
                  [(invoke-op 0 :read nil)
                   (ok-op     0 :read 1)])
           {:valid? false
            :reads  [[0 1 0]]
            :errors [[0 1 0]]})))

  (testing "interleaved concurrent reads and writes"
    (is (= (check counter nil nil
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
                   (ok-op     8 :read 15)])
           {:valid? false
            :reads  [[0 6 15] [0 0 15] [0 3 15] [0 100 15] [0 15 15]]
            :errors [[0 100 15]]})))

  (testing "rolling reads and writes"
    (is (= (check counter nil nil
                  [(invoke-op 0 :read nil)
                   (invoke-op 1 :add  1)
                   (ok-op     0 :read 0)
                   (invoke-op 0 :read nil)
                   (ok-op     1 :add  1)
                   (invoke-op 1 :add  2)
                   (ok-op     0 :read 3)
                   (invoke-op 0 :read nil)
                   (ok-op     1 :add  2)
                   (ok-op     0 :read 5)])
           {:valid? false
            :reads  [[0 0 1] [0 3 3] [1 5 3]]
            :errors [[1 5 3]]}))))

(deftest compose-test
  (is (= (check (compose {:a unbridled-optimism :b unbridled-optimism})
                nil nil nil)
         {:a {:valid? true}
          :b {:valid? true}
          :valid? true})))
