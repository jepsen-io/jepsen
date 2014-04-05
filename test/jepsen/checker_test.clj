(ns jepsen.checker-test
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

(deftest compose-test
  (is (= (check (compose {:a unbridled-optimism :b unbridled-optimism})
                nil nil nil)
         {:a {:valid? true}
          :b {:valid? true}
          :valid? true})))
