(ns jepsen.store-test
  (:refer-clojure :exclude [load test])
  (:use clojure.test)
  (:require [clojure.data.fressian :as fress]
            [clojure.string :as str]
            [fipp.edn :refer [pprint]]
            [jepsen.store :refer :all]
            [jepsen [common-test :refer [quiet-logging]]]
            [jepsen.core-test :as core-test]
            [jepsen.core :as core]
            [multiset.core :as multiset]
            [jepsen.tests :refer [noop-test]])
  (:import (org.fressian.handlers WriteHandler ReadHandler)))

(use-fixtures :once quiet-logging)

(defrecord Kitten [fuzz mew])

(def base-test (assoc noop-test
                      :pure-generators true
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
                      :atom     ["blah"]
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
  (let [name (:name base-test)
        _    (delete! name)
        t (-> base-test
              core/run!)
        [t serialized-t]
        (with-writer t [writer]
          (let [t (-> t
                      (save-1! writer)
                      (assoc-in [:results :kitten] (Kitten. "hi" "there"))
                      (save-2! writer))
                serialized-t (dissoc t :db :os :net :client :checker :nemesis
                                     :generator :model :remote)]
            [t serialized-t]))
        ts        (tests name)
        [time t'] (first ts)]
    (is (= 1 (count ts)))
    (is (string? time))

    (testing "generic test load"
      (is (= serialized-t @t')))
    (testing "test.fressian"
      (is (= serialized-t (load-fressian-file (fressian-file t)))))
    (testing "test.jepsen"
      (is (= serialized-t (load-jepsen-file (jepsen-file t)))))
    (testing "load-results"
      (is (= (:results t) (load-results name time))))
    (testing "results.edn"
      (is (= (:results t) (load-results-edn t))))))
