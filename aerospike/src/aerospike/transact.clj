(ns aerospike.transact
  "Tests MRTs"
  (:require [aerospike.support :as s]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client :as client]
             [independent :as independent]]
            [jepsen.tests.cycle.wr :as rw]))


(def txn-set "Set Name for Txn Test" "entries")

(defn txn-wp [tid]
  (let [p s/write-policy]
    (set! (.tran p) tid)
    p
    )
  )

(defn mop!
  "Given a client, transaction-id, and micro-operation,
   Executes micro-op invocation & Returns the completed micro-op."
  [conn tid [f k v]]
  [f k (case f
         :r (-> conn
                (s/fetch s/ans txn-set k tid)
                :bins
                :value
                (or []))
         :w (do 
              (let [wp (txn-wp tid)]
                (s/put! conn wp s/ans txn-set k {:value v})                
              )
              v)
        
  )]
  
  )

(defrecord TranClient [client namespace set]
  client/Client
  (open! [this _ node]
    (assoc this :client (s/connect node)))
  (setup! [this _] this)
  (invoke! [this test op]
    (info "Invoking" op)
    (if (= (:f op) :txn)   
      (s/with-errors op #{}
        (let [tid (.tranBegin client)]
        (try 
          (let [
              ;; wp (txn-wp tid)
                txn (:value op)
                txn' (mapv (partial mop! client tid) txn)
                ]
          ;; (info "TRANSACTION!" tid "begin")
          ;; (mapv (partial mop! client wp) txn)
          ;; (info "TRANSACTION!" tid "ending")
            (.tranCommit client tid)
            
            (assoc op :type :ok :value txn')
            )
          ;; (info  op)
          (catch com.aerospike.client.AerospikeException e#
            (info "Exception caught, aborting..")
            (.tranAbort client tid)
            (case (.getResultCode e#)
                29 (do
                     (info "CAUGHT CODE 29 in TranClient.invoke --> ABORTING " (:value op))
                     (assoc op :type :fail, :error :MRT-blocked)
                   )
            ;; 30
                (throw e#)
            )
          )
        )
      ))
      (info "REGULAR OP!"))
  )
  (teardown! [_ test])
  (close! [this test]
    (s/close client)))


(defn workload []
  {:client (TranClient. nil s/ans "vals")
   :checker (rw/checker)
   :generator (rw/gen {:key-dist :uniform, :key-count 3})}
)