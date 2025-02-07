(ns aerospike.transact
  "Tests MRTs"
  (:require [aerospike.support :as s]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [client :as client]]
            [jepsen.tests.cycle
             [wr :as rw]
             [append :as la]])
  (:import (com.aerospike.client Txn
                                 AerospikeException
                                 CommitStatus)))


(def txn-set "Set Name for Txn Test" "entries")

(defn txn-wp [tid]
  (let [p (s/write-policy)]
    (set! (.txn p) tid)
    p))

(defn mop!
  "Given a client, transaction-id, and micro-operation,
   Executes micro-op invocation & Returns the completed micro-op."
  [conn tid [f k v]]
  [f k (case f
         :r (-> conn
                (s/fetch s/ans txn-set k tid)
                :bins
                :value
                (or (throw (AerospikeException. 2 "RecordNotFound"))))
         :w (do
              (let [wp (txn-wp tid)]
                (s/put! conn wp s/ans txn-set k {:value v}))
              v)
         :append (let [wp (txn-wp tid)]
                   (s/list-append! conn wp s/ans txn-set k {:value v})
                   v))])

(defrecord TranClient [client namespace set]
  client/Client
  (open! [this _ node]
    (assoc this :client (s/connect node)))
  (setup! [this _] this)
  (invoke! [this test op]
    (info "Invoking" op)
    (if (= (:f op) :txn)
      (s/with-modern-errors op
        (let [tid (Txn.)
              txn' (atom nil)
              cs (atom nil)]
          (try
            (let [txn (:value op)
                  txn-res (mapv (partial mop! client tid) txn)]
              (reset! txn' txn-res)
           ;; (info "TRANSACTION!" tid "begin")
              (info "Txn: " (.getId tid) " ..DONE!")
              (reset! cs (.commit client tid))
              (when (not (= @cs CommitStatus/OK))
                (info "Commit Status := " cs))
              (assoc op :type :ok :value @txn'))

            (catch AerospikeException e#
              (info "Exception caught:" (.getResultCode e#) (.getMessage e#))
              (info "Aborting..")
              (try
                (.abort client tid)
                (catch AerospikeException e#
                  (if (= (.getResultCode e#) -18)
                    (do (info "<?IMPOSSIBLE?> ABORT AFTER COMMIT!")
                        (assoc op :type :ok, :value @txn'))
                    (throw e#))))
              (case (.getResultCode e#)
                -19 (assoc op :type :fail, :error :aborted-already)
                120 (assoc op :type :fail, :error :MRT-blocked)
                121 (assoc op :type :fail, :error :read-verify)
                122 (assoc op :type :fail, :error :expired)
                125 (assoc op :type :fail, :error :aborted)
                (throw e#))))))
      (info "<?IMPOSSIBLE?> REGULAR OP!")  ; Should never happen with txn test workloads 
    ))
  (teardown! [_ test])
  (close! [this test]
    (s/close client)))

(defn elle-gen-opts [opts]
  (let [kDist (:key-dist opts (rand-nth (list :exponential :uniform)))
        kCount (:key-count opts (if (= kDist :uniform)
                                  (rand-nth (range 3 8))
                                  (rand-nth (range 8 12))))
        minOps (:min-txn-length opts (rand-nth (range 1 6)))
        maxOps (:max-txn-length opts (rand-nth (range minOps 12)))]
    {:key-dist kDist
     :key-count kCount
     :min-txn-length minOps
     :max-txn-length maxOps
     :max-writes-per-key (:max-writes-per-key opts 32)}))

(defn workload
  [opts]
   {:client (TranClient. nil s/ans "vals")
    :checker (rw/checker)
    :generator (rw/gen
                (elle-gen-opts opts))})

(defn workload-ListAppend 
  [opts]
   {:client (TranClient. nil s/ans "vals")
    :checker (la/checker)
    :generator (la/gen
                (elle-gen-opts opts))})
