(ns jepsen.tests.cycle.core
  "Common functions for tests that do cycle detection using Elle."
  (:require [jepsen.generator :as gen]))

(defn max-key-tracker
  "A generator which tracks the maximum key observed in a :txn. Makes this
  integer key available in (:max-key ctx) to inner generators."
  [gen]
  (gen/track :max-key 0
             (fn [mk {:keys [f value]}]
               (if (= :txn f)
                 (->> value
                      (map second)
                      (reduce max mk))
                 mk))
             gen))

(defrecord FinalGen []
  gen/Generator
  (op [this test ctx]
    (when-let [mk (:max-key ctx)]
      (gen/op (->> (range (:max-key ctx))
                   (partition 8)
                   (map (fn [ks]
                          {:f :txn, :value (mapv (fn [k] [:r k nil]) ks)})))
              test ctx)))

  (update [this test ctx op]
    this))

(defn final-gen
  "Uses the max-key-tracker to emit a series of transactions which perform
  final reads of all the keys ever used."
  []
  (FinalGen.))
