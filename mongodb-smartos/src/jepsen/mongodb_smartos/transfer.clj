(ns jepsen.mongodb-smartos.transfer
  "Simulates bank account transfers between N accounts, as per
  From http://docs.mongodb.org/manual/tutorial/perform-two-phase-commits/"
  (:refer-clojure :exclude [read])
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
            [jepsen.mongodb-smartos.core :refer :all]
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
  (let [txn (from-db-object
              (mc/insert-and-return db txns
                                    {:_id (ObjectId.)
                                     :state       "pending"
                                     :from        (:from txn)
                                     :to          (:to txn)
                                     :amount      (:amount txn)}
                                    write-concern)
              true)]
    ; jfc mongo error handling is incomprehensibly inconsistent
    ; - What does insert-and-return return for failure?
    ; - Why doesn't insert-and-return return something that responds to
    ;   result/ok? for the successful case?
    (assert (map? txn))
    (assert (= #{:_id :state :from :to :amount} (set (keys txn))))
    txn))

(defn p1-find-txn
  "Finds a single transaction ID in the INITIAL state."
  [db txns]
  (:_id (mc/find-one-as-map db txns {:state "initial"})))

(defn p2-begin-txn
  "Updates the given transaction from initial to pending. Returns the
  transaction."
  [db txns write-concern txn]
  (parse-result
    (mc/update db txns
               {:_id (:_id txn), :state "initial"}
               {$set           {:state "pending"}
                "$currentDate" {:lastModified true}}
               {:write-concern write-concern})
    txn))

(defn p3-apply-txn
  "Applies the given transaction to both accounts. Returns the transaction."
  [db accts write-concern txn]
  (parse-result
    (mc/update db accts
               {:_id (:from txn), :pendingTxns {$ne (:_id txn)}}
               {$inc  {:balance (- (:amount txn))}
                $push {:pendingTxns (:_id txn)}}
               {:write-concern write-concern}))
  (parse-result
    (mc/update db accts
               {:_id (:to txn), :pendingTxns {$ne (:_id txn)}}
               {$inc  {:balance (:amount txn)}
                $push {:pendingTxns (:_id txn)}}
               {:write-concern write-concern}))
  txn)

(defn p4-applied-txn
  "Mark the transaction as successfully applied. Returns the transaction."
  [db txns write-concern txn]
  (parse-result
    (mc/update db txns
               {:_id (:_id txn), :state "pending"}
               {$set {:state "applied"}
                "$currentDate" {:lastModified true}}
               {:write-concern write-concern}))
  txn)

(defn p5-clear-pending
  "Clear the given transaction from the relevant account pending txn lists.
  Returns the transaction."
  [db accts write-concern txn]
  (parse-result
    (mc/update db accts
               {:_id (:from txn), :pendingTxns (:_id txn)}
               {$pull {:pendingTxns (:_id txn)}}
               {:write-concern write-concern}))
  (parse-result
    (mc/update db accts
               {:_id (:to txn), :pendingTxns (:_id txn)}
               {$pull {:pendingTxns (:_id txn)}}
               {:write-concern write-concern}))
  txn)

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
        (upsert db accts {:_id          id
                          :balance      starting-balance
                          :pendingTxns  []}
                write-concern))
      (assoc this :conn conn, :db db)))

  (invoke! [this test op]
    (with-errors op #{:read}
      (case (:f op)
        :read (->> (mq/with-collection db accts
                     (mq/find {})
                     (mq/fields [:_id :balance])
                     (mq/read-preference (ReadPreference/primary)))
                   (map (juxt :_id :balance))
                   (into {})
                   (assoc op :type :ok, :value))

        :partial-read (->> (mq/with-collection db accts
                             (mq/find {:pendingTxns {$size 0}})
                             (mq/read-preference (ReadPreference/primary)))
                           (map (juxt :_id :balance))
                           (into {})
                           (assoc op :type :ok, :value))

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
  "A client which transfers balances between a pool of n accounts, each
  beginning with starting-balance."
  [n starting-balance write-concern]
  (Client. "jepsen"
           "txns"
           "accts"
           (vec (range n))
           starting-balance
           write-concern
           nil
           nil))

(defrecord Accounts [accts]
  Model
  (step [m op]
    (let [value (:value op)]
      (try
        (condp = (:f op)
          :read (if (= accts value)
                  m
                  (knossos/inconsistent
                    (str "Can't read " value " from " accts)))

          :partial-read (if (every? true?
                                    (for [[acct-id balance] value]
                                      (= balance (get accts acct-id))))
                          m
                          (knossos/inconsistent
                            (str value " isn't consistent with " accts)))

          :transfer (let [{:keys [from to amount]} value]
                      (Accounts.
                        (assoc accts
                               from (- (get accts from) amount)
                               to   (+ (get accts to)   amount)))))
        (catch Throwable t
          (warn "Account model error:" :accts accts :op op)
          (throw t))))))

(defn account-model
  "Given a map of account IDs to balances, models transfers between those
  accounts."
  [accts]
  (Accounts. accts))

; Generators
(defn read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn partial-read
  "Reads the state of all accounts without a transaction in progress."
  [_ _]
  {:type :invoke, :f :partial-read})

(defn transfer
  "Transfers a random amount between two randomly selected accounts."
  [test process]
  (let [acct-ids (-> test :client :acct-ids)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-nth acct-ids)
             :to     (rand-nth acct-ids)
             :amount (rand-int 5)}}))

(def diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer))

(defn transfer-test
  "Generic transfer test"
  [name opts]
  (let [n                 3
        starting-balance  10]
    (test- (str "transfer " name)
           (merge {:client (client n starting-balance WriteConcern/JOURNAL_SAFE)
                   :model  (account-model (->> starting-balance
                                               (repeat n)
                                               (map-indexed vector)
                                               (into {})))}
                  opts))))

; Tests
(defn basic-read-test
  "Transfer test with a mix of simple reads and transfers."
  []
  (transfer-test "basic read"
                 {:generator (std-gen (gen/mix [read transfer]))}))

(defn partial-read-test
  "Reads can only take place on records which don't have any pending
  transactions."
  []
  (transfer-test "partial read"
                 {:generator (std-gen (gen/mix [partial-read transfer]))}))

(defn diff-account-test
  "Partial reads and only different-account transfers."
  []
  (transfer-test "diff account"
                 {:generator (std-gen (gen/mix [partial-read diff-transfer]))}))
