(ns jecci.common.sequential
  "A sequential consistency test.

  Verify that client order is consistent with DB order by performing queries
  (in four distinct transactions) like

  A: insert x
  A: insert y
  B: read y
  B: read x

  A's process order enforces that x must be visible before y; we should never
  observe y alone.

  Splits keys up onto different tables to make sure they fall in different
  shard ranges"
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [core :as jepsen]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh]]]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [jecci.common.basic :as basic]
            [knossos.model :as model]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [jecci.interface.client :as ic]))

(defn subkeys
  "The subkeys used for a given key, in order."
  [key-count k]
  (mapv (partial str k "_") (range key-count)))

(defn trailing-nil?
  "Does the given sequence contain a nil anywhere after a non-nil element?"
  [coll]
  (some nil? (drop-while nil? coll)))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (assert (integer? (:key-count test)))
      (let [reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %)))
                       (r/map :value)
                       (into []))
            none (filter (comp (partial every? nil?) second) reads)
            some (filter (comp (partial some nil?) second) reads)
            bad  (filter (comp trailing-nil? second) reads)
            all  (filter (fn [[k ks]]
                           (= (subkeys (:key-count test) k)
                              (reverse ks)))
                         reads)]
        {:valid?      (not (seq bad))
         :all-count   (count all)
         :some-count  (count some)
         :none-count  (count none)
         :bad-count   (count bad)
         :bad         bad}))))

(defrecord Writes [k last-written]
  gen/Generator
  (op [this test ctx]
    (let [_k (swap! k inc)
          _ (swap! last-written #(-> % pop (conj _k)))
          ]
      [(gen/fill-in-op {:type :invoke, :f :write, :value _k} ctx) this]))

  (update [this test ctx event]
    this))

(defn writes
  "We emit sequential integer keys for writes, logging the most recent n keys
  in the given atom, wrapping a PersistentQueue."
  [last-written]
  (Writes. (atom -1) last-written))

(defrecord Reads [last-written]
  gen/Generator
  (op [this test ctx]
    [(gen/fill-in-op {:type :invoke, :f :read, :value (rand-nth @last-written)} ctx) this]
    )
  (update [this test ctx event]
    this))

(defn reads
  "We use the last-written atom to perform a read of a randomly selected
  recently written value."
  [last-written]
  (gen/filter (comp complement nil? :value)
    (Reads. last-written)))

(defn gen
  "Basic generator with n writers, and a buffer of 2n"
  [n]
  (let [last-written (atom
                      (reduce conj clojure.lang.PersistentQueue/EMPTY
                              (repeat (* 2 n) nil)))]
    (gen/reserve n (writes last-written) (reads last-written))))

(defn workload
  [opts]
  (let [c         (:concurrency opts)
        gen       (gen (/ c 2))
        keyrange (atom {})]
      {:key-count 5
       :keyrange  keyrange
       :client    (ic/gen-SequentialClient c (atom false) nil)
       :generator (gen/stagger 1/100 gen)
       :checker   (checker)}))

