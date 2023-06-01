(ns yugabyte.set
  "Adds elements to sets and reads them back"
  (:require [jepsen.generator :as gen]
            [jepsen.checker :as checker]
            [yugabyte.generator :as ygen]))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       (map gen/once)))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn workload
  [opts]
  (let [threads  (:concurrency opts)]
    {:generator (->> (gen/reserve (/ threads 2) (adds)
                                  reads)
                     (gen/stagger (/ 1 threads))
                     (ygen/with-op-index))
     :checker   (checker/set-full)}))
