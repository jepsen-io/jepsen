(ns jepsen.store-test
  (:refer-clojure :exclude [load])
  (:use clojure.test
        clojure.pprint
        jepsen.store)
  (:require [jepsen.core-test :as core-test]
            [jepsen.core :as core]
            [multiset.core :as multiset]
            [jepsen.tests :refer [noop-test]]))

(defrecord Kitten [fuzz mew])

(def base-test (assoc noop-test
                      :name     "store-test"
                      :record   (Kitten. "fluffy" "smol")
                      :multiset (into (multiset/multiset)
                                      [1 1 2 3 5 8])))

(deftest ^:integration roundtrip-test
  (delete! "store-test")

  (let [t (-> base-test
              core/run!
              save-1!
              (assoc-in [:results :kitten] (Kitten. "hi" "there"))
              save-2!)]
    (let [ts     (tests "store-test")
          [k t'] (first ts)]
      (is (= 1 (count ts)))
      (is (string? k))
      (testing "test.fressian"
        (is (= @t'
               (dissoc t :db :os :net :client :checker
                       :nemesis :generator :model))))
      (testing "results.edn"
        (is (= (:results t) (load-results "store-test" k)))))))
