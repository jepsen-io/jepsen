(ns yugabyte.single-key-acid
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.independent :as independent]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]))


(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn workload
  [opts]
  (let [n (count (:nodes opts))]
    {:generator (independent/concurrent-generator
                  (* 2 n)
                  (range)
                  (fn [k]
                    (->> (gen/reserve n (gen/mix [w cas cas]) r)
                         (gen/stagger 1)
                         (gen/process-limit 20))))
     :checker   (independent/checker
                  (checker/compose
                    {:timeline (timeline/html)
                     :linear   (checker/linearizable
                                 {:model (model/cas-register 0)})}))}))
