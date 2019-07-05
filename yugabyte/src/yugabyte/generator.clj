(ns yugabyte.generator
  (:require [jepsen.generator :as gen]))

(defn with-op-index
  "Append :op-index integer to every operation emitted by the given generator.
  Value starts at 1 and increments by 1 for every subsequent emitted operation."
  [gen]
  (let [ctr          (atom 1)
        add-index-fn (fn [op] (locking ctr
                                (let [old-val @ctr]
                                  (compare-and-set! ctr old-val (inc old-val))
                                  (assoc op :op-index old-val))))]
    (gen/map add-index-fn gen)))

(defn workload-with-op-index
  "Alters a workload map, wrapping generator in with-op-index"
  [workload]
  (assoc workload :generator (with-op-index (:generator workload))))
