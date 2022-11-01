(ns jepsen.store-test
  (:refer-clojure :exclude [load test])
  (:use clojure.test)
  (:require [clojure.data.fressian :as fress]
            [clojure.string :as str]
            [fipp.edn :refer [pprint]]
            [jepsen [common-test :refer [quiet-logging]]
                    [core :as core]
                    [core-test :as core-test]
                    [generator :as gen]
                    [history :as history :refer [op]]
                    [store :refer :all]
                    [tests :refer [noop-test]]]
            [jepsen.store [format :as store.format]
                          [fressian :as store.fressian]]
            [multiset.core :as multiset])
  (:import (org.fressian.handlers WriteHandler ReadHandler)))

(use-fixtures :once quiet-logging)

(defrecord Kitten [fuzz mew])

(def base-test (assoc noop-test
                      :pure-generators true
                      :name     "store-test"
                      :generator (->> [{:f :trivial}]
                                      gen/clients)
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
                      :ops      [(op {:time 3, :index 4, :process :nemesis, :f
                                      :foo, :value [:hi :there]})]
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
        x' (with-open [in (fress/to-input-stream b)
                       r  (store.fressian/reader in)]
             (fress/read-object r))]
    x'))

(deftest fressian-test
  (are [x] (= x (fr x))
       #{1 2 3}
       [#{5 6}
        #{:foo}]))

(deftest fressian-vector-test
  ; Make sure we decode these as vecs, not arraylists.
  (is (vector? (fr [])))
  (is (vector? (fr [1])))
  (is (vector? (fr [:x :y])))
  (is (vector? (:foo (fr {:foo [:x :y]})))))

(deftest ^:integration roundtrip-test
  (let [name (:name base-test)
        _    (delete! name)
        t (-> base-test
              core/run!)
        ; At this juncture we've run the test, and the history should be
        ; written.
        t' (load t)
        _ (is (= (:history t) (:history t')))
        _ (is (instance? jepsen.history.Op (first (:history t))))
        _ (is (instance? jepsen.history.Op (first (:history t'))))

        ; Now we're going to rewrite the results, adding a kitten
        [t serialized-t]
        (with-handle [t t]
          (let [t (-> t
                      (assoc-in [:results :kitten] (Kitten. "hi" "there"))
                      save-2!)
                serialized-t (dissoc t :db :os :net :client :checker :nemesis
                                     :generator :model :remote :store)]
            [t serialized-t]))
        ts        (tests name)
        [time t'] (first ts)]
    (is (= 1 (count ts)))
    (is (string? time))

    (testing "generic test load"
      (is (= serialized-t @t')))
    (testing "test.jepsen"
      (is (= serialized-t (load-jepsen-file (jepsen-file t)))))
    (testing "load-results"
      (is (= (:results t) (load-results name time))))
    (testing "results.edn"
      (is (= (:results t) (load-results-edn t))))))
