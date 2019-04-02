(ns tidb.bank
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen
             [client :as client]
             [generator :as gen]
             [checker :as checker]]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [clojure.java.jdbc :as j]
            [tidb.sql :refer :all]
            [tidb.basic :as basic]
            [clojure.tools.logging :refer :all]))

(defrecord BankClient [node n starting-balance lock-type in-place?]
  client/Client
  (open! [this test node]
    (assoc this :node node))

  (setup! [this test]
    (j/with-db-connection [c (conn-spec (first (:nodes test)))]
      (j/execute! c ["create table if not exists accounts
                     (id     int not null primary key,
                     balance bigint not null)"])
      (dotimes [i n]
        (try
          (with-txn-retries
            (j/insert! c :accounts {:id i, :balance starting-balance}))
          (catch java.sql.SQLIntegrityConstraintViolationException e nil)))))

  (invoke! [this test op]
    (with-txn op [c (first (:nodes test))]
      (try
        (case (:f op)
          :read (->> (j/query c [(str "select * from accounts")])
                     (mapv :balance)
                     (assoc op :type :ok, :value))
          :transfer
          (let [{:keys [from to amount]} (:value op)
                b1 (-> c
                       (j/query [(str "select * from accounts where id = ?" lock-type) from]
                                :row-fn :balance)
                       first
                       (- amount))
                b2 (-> c
                       (j/query [(str "select * from accounts where id = ?"
                                      lock-type)
                                 to]
                                :row-fn :balance)
                       first
                       (+ amount))]
            (cond (neg? b1)
                  (assoc op :type :fail, :value [:negative from b1])
                  (neg? b2)
                  (assoc op :type :fail, :value [:negative to b2])
                  true
                  (if in-place?
                    (do (j/execute! c ["update accounts set balance = balance - ? where id = ?" amount from])
                        (j/execute! c ["update accounts set balance = balance + ? where id = ?" amount to])
                        (assoc op :type :ok))
                    (do (j/update! c :accounts {:balance b1} ["id = ?" from])
                        (j/update! c :accounts {:balance b2} ["id = ?" to])
                        (assoc op :type :ok)))))))))

  (teardown! [_ test])

  (close! [_ test]))

(defn bank-client
  "Simulates bank account transfers between n accounts, each starting with
  starting-balance."
  [n starting-balance lock-type in-place?]
  (BankClient. nil n starting-balance lock-type in-place?))

(defn bank-read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [test process]
  (let [n (-> test :client :n)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-int n)
             :to     (rand-int n)
             :amount (rand-int 5)}}))

(def bank-diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              bank-transfer))

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  [model]
  (reify checker/Checker
    (check [this test history opts]
      (let [bad-reads (->> history
                           (r/filter op/ok?)
                           (r/filter #(= :read (:f %)))
                           (r/map (fn [op]
                                    (let [balances (:value op)]
                                      (cond (not= (:n model) (count balances))
                                            {:type :wrong-n
                                             :expected (:n model)
                                             :found    (count balances)
                                             :op       op}
                                            (not= (:total model)
                                                  (reduce + balances))
                                            {:type :wrong-total
                                             :expected (:total model)
                                             :found    (reduce + balances)
                                             :op       op}))))
                           (r/filter identity)
                           (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defn bank-test-base
  [opts]
  (basic/basic-test
    (merge
      {:client      {:client (:client opts)
                     :during (->> (gen/mix [bank-read bank-diff-transfer])
                                  (gen/clients))
                     :final (gen/clients (gen/once bank-read))}
       :checker     (checker/compose
                      {:perf    (checker/perf)
                       :details (bank-checker {:n 5 :total 50})})}
      (dissoc opts :client))))

(defn test
  [opts]
  (bank-test-base
    (merge {:name   "bank"
            :client (bank-client 5 10 " FOR UPDATE" false)}
           opts)))

; One bank account per table
(defrecord MultiBankClient [node tbl-created? n starting-balance lock-type in-place?]
  client/Client
  (setup! [this test node]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (j/with-db-connection [c (conn-spec (first (:nodes test)))]
          (dotimes [i n]
            (Thread/sleep 500)
            (info "Creating table accounts" i)
            (j/execute! c [(str "create table if not exists accounts" i
                                "(id     int not null primary key,"
                                "balance bigint not null)")])
            (Thread/sleep 500)
            (try
              (Thread/sleep 500)
              (info "Populating account" i)
              (with-txn-retries
                (j/insert! c (str "accounts" i) {:id 0, :balance starting-balance}))
              (catch java.sql.SQLIntegrityConstraintViolationException e nil))))))

    (assoc this :node node))

  (invoke! [this test op]
    (with-txn op [c (first (:nodes test))]
      (try
        (case (:f op)
          :read
          (->> (range n)
               (mapv (fn [x]
                       (->> (j/query
                             c [(str "select balance from accounts" x)]
                             :row-fn :balance)
                            first)))
               (assoc op :type :ok, :value))
          :transfer
          (let [{:keys [from to amount]} (:value op)
                from (str "accounts" from)
                to   (str "accounts" to)
                b1 (-> c
                       (j/query
                        [(str "select balance from " from lock-type)]
                        :row-fn :balance)
                       first
                       (- amount))
                b2 (-> c
                       (j/query [(str "select balance from " to lock-type)]
                                :row-fn :balance)
                       first
                       (+ amount))]
            (cond (neg? b1)
                  (assoc op :type :fail, :error [:negative from b1])
                  (neg? b2)
                  (assoc op :type :fail, :error [:negative to b2])
                  true
                  (if in-place?
                    (do (j/execute! c [(str "update " from " set balance = balance - ? where id = 0") amount])
                        (j/execute! c [(str "update " to " set balance = balance + ? where id = 0") amount])
                        (assoc op :type :ok))
                    (do (j/update! c from {:balance b1} ["id = 0"])
                        (j/update! c to {:balance b2} ["id = 0"])
                        (assoc op :type :ok)))))))))

  (teardown! [_ test]))

(defn multitable-bank-client
  [n starting-balance lock-type in-place?]
  (MultiBankClient. nil (atom false) n starting-balance lock-type in-place?))

(defn multitable-test
  [opts]
  (bank-test-base
   (merge {:name "bank-multitable"
           :model {:n 5 :total 50}
           :client (multitable-bank-client 5 10 " FOR UPDATE" false)}
          opts)))
