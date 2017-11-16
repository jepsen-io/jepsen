(ns aerospike.counter
  "Incrementing a counter"
  (:require [aerospike.support :as s]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [dom-top.core :refer [with-retry letr]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis   :as nemesis]
                    [os        :as os]
                    [store     :as store]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.time :as nt]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [wall.hack])
  (:import (clojure.lang ExceptionInfo)
           (com.aerospike.client AerospikeClient
                                 AerospikeException
                                 AerospikeException$Connection
                                 AerospikeException$Timeout
                                 Bin
                                 Info
                                 Key
                                 Record)
           (com.aerospike.client.cluster Node)
           (com.aerospike.client.policy Policy
                                        ConsistencyLevel
                                        GenerationPolicy
                                        WritePolicy)))

(defrecord CounterClient [client namespace set key]
  client/Client
  (setup! [this test node]
    (let [client (s/connect node)]
      (Thread/sleep 3000) ; TODO: remove?
      (s/put! client namespace set key {:value 0})
      (assoc this :client client)))

  (invoke! [this test op]
    (s/with-errors op #{:read}
      (case (:f op)
        :read (assoc op :type :ok
                     :value (-> client (s/fetch namespace set key) :bins :value))

        :add  (do (s/add! client namespace set key {:value (:value op)})
                  (assoc op :type :ok)))))

  (teardown! [this test]
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
