(ns jepsen.generator.translation-table-test
  (:require [clojure [test :refer :all]
                     [pprint :refer :all]]
            [jepsen.generator.translation-table :refer :all])
  (:import (java.util BitSet)))

(deftest basic-test
  (let [t (translation-table 2 [:nemesis])]
    (is (= [0 1 :nemesis] (all-names t)))
    (is (= 3 (thread-count t)))
    (testing "name->index"
      (is (= 0 (name->index t 0)))
      (is (= 1 (name->index t 1)))
      (is (= 2 (name->index t :nemesis))))
    (testing "index->name"
      (is (= 0 (index->name t 0)))
      (is (= 1 (index->name t 1)))
      (is (= :nemesis (index->name t 2))))
    (testing "bitset slices"
      (let [bs (doto (BitSet.) (.set 1) (.set 2))]
        (is (= #{1 :nemesis} (set (indices->names t bs))))
        (is (= bs (names->indices t #{1 :nemesis})))))))
