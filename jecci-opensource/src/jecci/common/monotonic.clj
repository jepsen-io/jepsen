(ns jecci.common.monotonic
  "Establishes a collection of integers identified by keys. Monotonically
  increments individual keys via read-write transactions, and reads keys in
  small groups. We verify that the order of transactions implied by each key
  are mutually consistent; e.g. no transaction can observe key x increase, but
  key y decrease."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]
             [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]
            [elle
             [list-append :as la]
             [rw-register :as rr]
             [txn :as et]
             [core :as ec]]
            [jecci.common.txn :as txn]
            [jecci.interface.client :as ic]))


(defn reads [key-count]
  (fn [] {:type  :invoke
          :f     :read
          :value (-> (range key-count)
                   ;util/random-nonempty-subset
                   (zipmap (repeat nil)))}))

(defn incs [key-count]
  (fn [] {:type :invoke,
          :f :inc
          :value (rand-int key-count)}))

(defn inc-workload
  [opts]
  (let [key-count 8]
    {:client (ic/gen-IncrementClient nil)
     :checker (checker/compose
                {:cycle (cycle/checker
                          (ec/combine ec/monotonic-key-graph
                            ec/realtime-graph))
                 :timeline (timeline/html)})
     :generator (->> (gen/mix [(incs key-count)
                               (reads key-count)]))}))


(defn txn-workload
  [opts]
  {:client  (txn/client {:val-type "int"})
   :checker (cycle/checker
              (ec/combine rr/wr-graph
                ec/realtime-graph))
   :generator (->> (et/wr-txns {:min-txn-length 2, :max-txn-length 5})
                (map (fn [txn] {:type :invoke, :f :txn, :value txn})))})

(defn append-client
  "Wraps a TxnClient, translating string lists back into integers."
  [client]
  (reify client/Client
    (open! [this test node]
      (append-client (client/open! client test node)))

    (setup! [this test]
      (append-client (client/setup! client test)))

    (invoke! [this test op]
      (let [op' (client/invoke! client test op)
            txn' (mapv (fn [[f k v :as mop]]
                         (if (= f :r)
                           ; Rewrite reads to convert "1,2,3" to [1 2 3].
                           [f k (when v (mapv #(Long/parseLong %)
                                          (str/split v #",")))]
                           mop))
                   (:value op'))]
        (assoc op' :value txn')))

    (teardown! [this test]
      (client/teardown! client test))

    (close! [this test]
      (client/close! client test))))


(defn append-workload
  [opts]
  {:client (append-client (txn/client {:val-type "text"}))
   :generator (->> (la/append-txns {:min-txn-length      1
                                 :max-txn-length      4
                                 :key-count           5
                                 :max-writes-per-key  16})
                (map (fn [txn] {:type :invoke, :f :txn, :value txn}))
                )
   :checker (append/checker {:anomalies         [:G-single]
                             :additional-graphs [ec/realtime-graph]})})
