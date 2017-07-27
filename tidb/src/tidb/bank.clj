(ns tidb.bank
  (:require [clojure.string :as str]
            [jepsen.os.debian :as debian]
            [jepsen
              [client :as client]
              [tests :as tests]
              [generator :as gen]
              [checker :as checker]
            ]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [clojure.java.jdbc :as j]
            [tidb.client :refer :all]
            [tidb.db :as db]
  )
)

(defrecord BankClient [node n starting-balance lock-type in-place?]
  client/Client
  (setup! [this test node]
    (j/with-db-connection [c (conn-spec (first (:nodes test)))]
      (j/execute! c ["create table if not exists accounts
                     (id      int not null primary key,
                     balance bigint not null)"])
      (dotimes [i n]
        (try
          (with-txn-retries
            (j/insert! c :accounts {:id i, :balance starting-balance}))
          (catch java.sql.SQLIntegrityConstraintViolationException e nil))))

    (assoc this :node node))

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
)

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
  []
  (reify checker/Checker
    (check [this test model history opts]
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

(def n 2)
(def initial-balance 10)

(defn bank-test
  [opts]
  (merge tests/noop-test
    {:os debian/os
     :name "TiDB-Bank"
     :concurrency 20
     :model  {:n n :total (* n initial-balance)}
     :db (db/db opts)
     :client (bank-client n initial-balance " FOR UPDATE" false)
     :generator (gen/phases
                  (->> (gen/mix [bank-read bank-diff-transfer])
                       (gen/clients)
                       (gen/stagger 1/10)
                       (gen/time-limit 15))
                  (gen/log "waiting for quiescence")
                  (gen/sleep 10)
                  (gen/clients (gen/once bank-read)))
     :checker (checker/compose
                {:perf (checker/perf)
                 :bank (bank-checker)})

    }
  )
)
