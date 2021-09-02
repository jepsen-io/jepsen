(ns jepsen.txn-test
  (:require [clojure.test :refer :all]
            [criterium.core :refer [quick-bench bench with-progress-reporting]]
            [jepsen.txn :refer :all]))

(deftest ext-reads-test
  (testing "no ext reads"
    (is (= {} (ext-reads [])))
    (is (= {} (ext-reads [[:w :x 2] [:r :x 2]]))))

  (testing "some reads"
    (is (= {:x 2} (ext-reads [[:w :y 1] [:r :x 2] [:w :x 3] [:r :x 3]])))))

(deftest ext-writes-test
  (testing "no ext writes"
    (is (= {} (ext-writes [])))
    (is (= {} (ext-writes [[:r :x 1]]))))

  (testing "ext writes"
    (is (= {:x 1 :y 2} (ext-writes [[:w :x 1] [:r :y 0] [:w :y 1] [:w :y 2]])))))

(deftest ^:perf ext-reads-perf
  (with-progress-reporting
  (bench (ext-reads [[:w :y 1] [:r :x 2] [:w :x 3] [:r :x 3]]))))
