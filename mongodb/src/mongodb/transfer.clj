(ns mongodb.transfer
  "Simulates bank account transfers between N accounts, as per
  From http://docs.mongodb.org/manual/tutorial/perform-two-phase-commits/"
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [knossos.core :as knossos]
            [cheshire.core :as json]
            [mongodb.core :refer :all]
            [monger.core :as mongo]
            [monger.collection :as mc]
            [monger.result :as mr]
            [monger.query :as mq]
            [monger.command]
            [monger.operators :refer :all]
            [monger.conversion :refer [from-db-object]])
  (:import (knossos.core Model)
           (clojure.lang ExceptionInfo)
           (org.bson BasicBSONObject
                     BasicBSONDecoder)
           (org.bson.types ObjectId)
           (com.mongodb DB
                        WriteConcern
                        ReadPreference)))

(defn p0-create-txn
  "Creates a new transaction in state PENDING and returns it. txn should be a
  map with :from, :to, and :amount keys."
  [db txns write-concern txn]
  (parse-result
    (mc/insert db txns
               {:_id (ObjectId.)
                :state "pending"
                :from   (:from txn)
                :to     (:to txn)
                :amount (:amount txn)}
               write-concern)))

(defn p1-find-txn
  "Finds a single transaction ID in the INITIAL state."
  [db txns]
  (:_id (mc/find-one-as-map db txns {:state "initial"})))

(defn p2-begin-txn
  "Updates the given transaction id from initial to pending."
  [db txns write-concern id]
  (parse-result
    (mc/update db txns
               {:_id id, :state "initial"}
               {$set           {:state "pending"}
                "$currentDate" {:lastModified true}}
               {:write-concern write-concern})))

(defn p3-apply-txn
  "Applies the given transaction to both accounts. Returns the transaction ID."
  [db accts write-concern txn]
  (parse-result
    (mc/update db accts
               {:_id (:from txn), :pendingTransactions {$ne (:_id txn)}}
               {$inc  {:balance (- (:value txn))}
                $push {:pendingTransactions (:_id txn)}}
               {:write-concern write-concern}))
  (parse-result
    (mc/update db accts
               {:_id (:to txn), :pendingTransactions {$ne (:_id txn)}}
               {$inc  {:balance (:value txn)}
                $push {:pendingTransactions (:_id txn)}}
               {:write-concern write-concern}))
  (:_id txn))

(defn p4-applied-txn
  "Mark the transaction as successfully applied. Returns the transaction."
  [db txns write-concern id]
  (parse-result
    (mc/update db txns
               {:_id id, :state "pending"}
               {$set {:state "applied"}
                "$currentDate" {:lastModified true}}
               {:write-concern write-concern})))

(defn p5-clear-pending
  "Clear the given transaction from the relevant account pending txn lists.
  Returns the transaction ID."
  [db accts write-concern txn]
  (parse-result
    (mc/update db accts
               {:_id (:from txn), :pendingTransactions (:_id txn)}
               {$pull {:pendingTransactions (:_id txn)}}
               {:write-concern write-concern}))
  (parse-result
    (mc/update db accts
               {:_id (:to txn), :pendingTransactions (:_id txn)}
               {$pull {:pendingTransactions (:_id txn)}}
               {:write-concern write-concern})
    (:_id txn)))

(defn p6-finish-txn
  "Marks the transaction as complete."
  [db txns write-concern id]
  (parse-result
    (mc/update db txns
               {:_id id, :state "applied"}
               {$set {:state "done"}
                "$currentDate" {:lastModified true}}
               {:write-concern write-concern})))

(defrecord Client [db-name txns accts acct-ids starting-balance write-concern conn db]
  client/Client
  (setup! [this test node]
    (let [conn (cluster-client test)
         db     (mongo/get-db conn db-name)]
      ; Create accounts 
      (doseq [id acct-ids]
        (upsert db accts {:_id id :balance starting-balance} write-concern))
      (assoc this :conn conn, :db db)))

  (invoke! [this test op]
    (with-errors op #{:read}
      (case (:f op)
        :read (let [res (->> (mq/with-collection db accts
                               (mq/find {})
                               (mq/fields [:_id :balance])
                               (mq/read-preference (ReadPreference/primary)))
                             (map (juxt :_id :balance))
                             (into {}))]
                (assoc op :type :ok, :value res))

        :transfer (do (->> (:value op)
                           (p0-create-txn    db txns  write-concern)
                           (p3-apply-txn     db accts write-concern)
                           (p4-applied-txn   db txns  write-concern)
                           (p5-clear-pending db accts write-concern)
                           (p6-finish-txn    db accts write-concern))
                      (assoc op :type :ok)))))

  (teardown! [_ test]
    (mongo/disconnect conn)))

(defn client
  "A client which transfers balances between a pool of n accounts."
  [n starting-balance write-concern]
  (Client. "jepsen"
           "txns"
           "accts"
           (vec (range n))
           starting-balance
           write-concern
           nil
           nil))

(defn account-model
  "Given a map of account IDs to balances, models transfers between those
  accounts."
  [accts]
  (reify Model
    (step [m op]
      (condp = (:f op)
        :read (if (= accts (:value op))
                m
                (knossos/inconsistent
                  (str "Can't read " (:value op) " from " accts)))
        :transfer (let [{:keys [from to amount]} (:value op)]
                    (account-model
                      (assoc accts
                             from (- (get accts from) amount)
                             to   (+ (get accts to) amount))))))))

; Generators
(defn r [_ _] {:type :invoke, :f :read})
(defn t [test process]
  (let [acct-ids (-> test :client :acct-ids)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-nth acct-ids)
             :to     (rand-nth acct-ids)
             :amount (rand-int 5)}}))

(defn majority-test
  "Transfer with n (default 2) accounts and MAJORITY, excluding reads because
  mongo doesn't have linearizable reads."
  ([] (majority-test 2))
  ([n]
   (let [starting-balance 10]
     (test- "transfer majority"
            {:client    (client n WriteConcern/MAJORITY)
             :model     (account-model (->> starting-balance
                                            (repeat n)
                                            (map-indexed vector)
                                            (into {})))
             :generator (std-gen (gen/mix [r t]))}))))
