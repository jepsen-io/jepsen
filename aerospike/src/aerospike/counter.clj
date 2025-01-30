(ns aerospike.counter
  "Incrementing a counter"
  (:require [aerospike.support :as s]
            [jepsen 
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]]
            [wall.hack]))

(defrecord CounterClient [client namespace set key]
  client/Client
  (open! [this test node]
    (let [client (s/connect node)]
      (assoc this :client client)))

  (setup! [this test] this)

  (invoke! [this test op]
    (s/with-modern-errors op
      (case (:f op)
        :read (assoc op :type :ok
                     :value (-> client (s/fetch namespace set key) :bins :value))

        :add  (do (s/add! client namespace set key {:value (:value op)})
                  (assoc op :type :ok)))))

  (teardown! [this test])

  (close! [this test]
    (s/close client)))

(defn counter-client
  "A basic counter."
  []
  (CounterClient. nil s/ans "counters" "pounce"))

(defn r   [_ _] {:type :invoke, :f :read})
(defn add [_ _] {:type :invoke, :f :add, :value 1})

(defn workload
  []
  {:client    (counter-client)
   :generator (->> (repeat 100 add)
                   (cons r)
                   gen/mix
                   (gen/delay 1/100))
   :checker   (checker/counter)})
