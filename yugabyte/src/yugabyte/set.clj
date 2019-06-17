(ns yugabyte.set
  "Adds elements to sets and reads them back"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info]]
            [jepsen.generator :as gen]
            [jepsen.checker :as checker]))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn workload
  [opts]
  {:generator (->> (gen/reserve (/ (:concurrency opts) 2) (adds)
                                (reads))
                   (gen/stagger 1/10))
   :checker   (checker/set-full)})
