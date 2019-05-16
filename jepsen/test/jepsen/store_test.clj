(ns jepsen.store-test
  (:refer-clojure :exclude [load])
  (:use clojure.test
        clojure.pprint
        jepsen.store)
  (:require [clojure.data.fressian :as fress]
            [clojure.string :as str]
            [jepsen.core-test :as core-test]
            [jepsen.core :as core]
            [multiset.core :as multiset]
            [jepsen.tests :refer [noop-test]])
  (:import (org.fressian.handlers WriteHandler ReadHandler)))

(defrecord Kitten [fuzz mew])

(def base-test (assoc noop-test
                      :name     "store-test"
                      :record   (Kitten. "fluffy" "smol")
                      :multiset (into (multiset/multiset)
                                      [1 1 2 3 5 8])
                      :nil      nil
                      :boolean  false
                      :long     1
                      :double   1.5
                      :rational 5/7
                      :bignum   123M
                      :string   "foo"
                      :vec      [1 2 3]
                      :seq      (map inc [1 2 3])
                      :cons     (cons 1 (cons 2 nil))
                      :set      #{1 2 3}
                      :map      {:a 1 :b 2}
                      :sorted-map (sorted-map 1 :x 2 :y)
                      :plot {:nemeses
                             #{{:name "pause pd",
                                :color "#C5A0E9",
                                :start #{:pause-pd},
                                :stop #{:resume-pd}}}}))

(defn fr
  "Fressian roundtrip"
  [x]
  (let [b (fress/write x :handlers write-handlers)
        ;_  (hexdump/print-dump (.array b))
        x' (fress/read b :handlers read-handlers)]
    x'))

(deftest fressian-test
  (are [x] (= x (fr x))
       #{1 2 3}
       [#{5 6}
        #{:foo}]))

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
        (is (= (dissoc t :db :os :net :client :checker
                       :nemesis :generator :model)
               @t')))
      (testing "results.edn"
        (is (= (:results t) (load-results "store-test" k)))))))
