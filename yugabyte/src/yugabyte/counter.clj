(ns yugabyte.counter
  (:require [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.checker.timeline :as timeline]
            [yugabyte.generator :as ygen]))


(defn add []  {:type :invoke :f :add :value 1})
(defn sub []  {:type :invoke :f :add :value -1})
(defn r   []  {:type :invoke :f :read})

(defn workload
  [opts]
  {:generator (->> (repeat 100 add)
                   (cons r)
                   gen/mix
                   (gen/delay 1/10)
                   (ygen/with-op-index))
   :checker   (checker/compose
                {:timeline (timeline/html)
                 :counter  (checker/counter)})})

(defn workload-dec
  [opts]
  (assoc (workload opts)
    :generator (->> (take 100 (cycle [add sub]))
                    (cons r)
                    gen/mix
                    (gen/delay 1/10)
                    (ygen/with-op-index))))
