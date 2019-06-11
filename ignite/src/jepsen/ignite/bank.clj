(ns jepsen.ignite.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
    (:require [clojure.tools.logging   :refer :all]
              [jepsen [ignite          :as ignite]
                      [checker         :as checker]
                      [client          :as client]
                      [nemesis         :as nemesis]
                      [generator       :as gen]]
              [clojure.core.reducers :as r]
              [jepsen.checker.timeline :as timeline]
              [knossos.model           :as model]
              [knossos.op :as op])
    (:import client.Bank
              (org.apache.ignite.transactions TransactionTimeoutException)
              (org.apache.ignite.cache CacheMode CacheAtomicityMode CacheWriteSynchronizationMode)))

(def accounts 10)
(def account-balance 100)

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [bad-reads (->> history
                        (r/filter op/ok?)
                        (r/filter #(= :read (:f %)))
                        (r/map (fn [op]
                          (let [balances (:value op)]
                            (cond
                              (not= accounts (count (keys balances)))
                              {:type :wrong-n
                               :expected accounts
                               :found    (count (keys balances))
                               :op       op}

                              (not= (* accounts account-balance) (reduce + (vals balances)))
                              {:type :wrong-total
                               :expected (* accounts account-balance)
                               :found   (reduce + (vals balances))
                               :op       op}

                              (some neg? (vals balances))
                              {:type     :negative-value
                               :found    (vals balances)
                               :op       op}))))
                       (r/filter identity)
                       (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defrecord BankClient
  [cache-initialised?
   ignite-config-file
   conn
   cache-config
   transaction-config]
  client/Client
  (open! [this test node]
    (let [ignite-config-file (ignite/configure-client (:nodes test) (:pds test))
          conn               (Bank. (.getCanonicalPath ignite-config-file))]
      (.setAccountCache
       conn
       (:atomicity-mode   cache-config)
       (:cache-mode       cache-config)
       (:write-sync-mode  cache-config)
       (:read-from-backup cache-config)
       (:backups          cache-config))
       (assoc this :conn conn)))
  (setup! [this test]
    (locking cache-initialised?
      (when (compare-and-set! cache-initialised? false true)
        (dotimes [i accounts]
          (info "Creating account" i)
          (.updateAccountBalance conn i account-balance)))))
  (invoke! [_ test op]
    (try
      (case (:f op)
        :read (let [value (.getAllAccounts conn accounts (:concurrency transaction-config) (:isolation transaction-config))]
                (assoc op :type :ok, :value value))
        :transfer (let [{:keys [from to amount]} (:value op)
                    amount (.transferMoney conn from to (:concurrency transaction-config) (:isolation transaction-config))]
                    (assoc op :type :ok)))
      (catch Exception e (info (.getMessage e)) (assoc op :type :fail, :error :exception))))
  (teardown! [this test])
  (close! [this test]
    (.stop conn)))

(defn bank-read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [_ _]
  {:type  :invoke
   :f     :transfer
   :value {:from (long (rand-int accounts))
           :to   (long (rand-int accounts))}})

(def bank-diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter
  (fn [op]
    (not= (-> op :value :from)
          (-> op :value :to)))
  bank-transfer))

(defn test
  [opts]
  (ignite/basic-test
    (merge
      {:name      "bank-test"
       :client    (BankClient. (atom false) nil nil (ignite/get-cache-config opts) (ignite/get-transaction-config opts))
       :checker   (checker/compose
                    {:perf     (checker/perf)
                     :timeline (timeline/html)
                     :details  (bank-checker)})
       :generator (ignite/generator [bank-diff-transfer bank-read] (:time-limit opts))}
      opts)))
