(ns yugabyte.counter
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.checker.timeline :as timeline]))


(def add {:type :invoke :f :add :value 1})
(def sub {:type :invoke :f :add :value -1})
(def r   {:type :invoke :f :read})

(defn workload
  [opts]
  {:generator (->> (repeat 100 add)
                   (cons r)
                   gen/mix
                   (gen/delay 1/10))
   :checker   (checker/compose
                {:timeline (timeline/html)
                 :counter  (checker/counter)})})

(defn workload-dec
  [opts]
  (assoc (workload opts)
    :generator (->> (take 100 (cycle [add sub]))
                    (cons r)
                    gen/mix
                    (gen/delay 1/10))))
