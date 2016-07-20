(ns jepsen.store-test
  (:refer-clojure :exclude [load])
  (:use clojure.test
        clojure.pprint
        jepsen.store)
  (:require [jepsen.core-test :as core-test]
            [jepsen.core :as core]
            [multiset.core :as multiset]
            [jepsen.tests :refer [noop-test]]))

(deftest roundtrip-test
  (delete! "store-test")

  (let [t (core/run! (assoc noop-test
                            :name     "store-test"
                            :multiset (into (multiset/multiset)
                                            [1 1 2 3 5 8])))]
    (save-1! t)
    (let [ts     (tests "store-test")
          [k t'] (first ts)]
      (is (= 1 (count ts)))
      (is (string? k))
      (is (= @t'
             (dissoc t :db :os :net :client :checker :nemesis :generator :model))))))
