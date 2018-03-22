(ns jepsen.faunadb.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
  (:use jepsen.faunadb.query)
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [reconnect :as rc]
                    [util :as util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.faunadb.client :as f]
            [jepsen.fauna :as fauna]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op]))

(def classRef
  "Accounts class ref"
  (ClassRef (v "accounts")))

(def balancePath
  "Path to balance data"
  (Arr (v "data") (v "balance")))

(defrecord BankClient [tbl-created? n starting-balance conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (f/query
          conn
          (Do
            (If
              (Exists classRef)
              (Delete classRef)
              (Obj "foo" (v "bar")))
            (CreateClass (Obj "name" (v "accounts")))))

          (dotimes [i n]
            (info "Creating account" i)
            (f/query
              conn
              (Create
                (Ref classRef i)
                (Obj "data" (Obj "balance" (v starting-balance)))))))))

  (invoke! [this test op]
    (case (:f op)
      :read
      (let [n (:value op)]
        (->>
          (mapv
            (fn [i]
             (f/get
               conn
               (Select
                 balancePath
                 (Get (Ref classRef i)))
               f/LongField))
            (range n))
          (assoc op :type :ok, :value)))

      :transfer
      (let [{:keys [from to amount]} (:value op)]
        (f/query
          conn
          (Do
            (Let
              {"a" (Subtract
                     (Select
                       balancePath
                       (Get (Ref classRef from)))
                     amount)}
              (If
                (Or (LessThan (Var "a") 0))
                (Abort "balance would go negative")
                (Update
                  (Ref classRef from)
                  (Obj "data" (Obj "balance" (Var "a"))))))
            (Let
              {"b" (Add
                     (Select
                       balancePath
                       (Get (Ref classRef to)))
                     amount)}
              (Update
                (Ref classRef to)
                (Obj "data" (Obj "balance" (Var "b")))))))
        (assoc op :type :ok))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn bank-read
  "Reads the current state of all accounts"
  [test _]
  (let [n (-> test :client :n)]
    {:type  :invoke,
     :f     :read,
     :value n}))

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [test process]
  (let [n (-> test :client :n)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-int n)
             :to     (rand-int n)
             :amount (+ 1 (rand-int 5))}}))

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
                                             :op       op}

                                            (some neg? balances)
                                            {:type     :negative-value
                                             :found    balances
                                             :op       op}
                                            ))))
                           (r/filter identity)
                           (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defn bank-test-base
  [opts]
  (fauna/basic-test
    (merge
      {:client      {:client (:client opts)
                     :during (->> (gen/mix [bank-read bank-diff-transfer])
                                  (gen/clients)
                                  (gen/stagger 0))
                     :final (gen/clients (gen/once bank-read))}
       :checker     (checker/compose
                      {:perf    (checker/perf)
                       :timeline (timeline/html)
                       :details (bank-checker)})}
      (dissoc opts :client))))

(defn test
  [opts]
  (bank-test-base
    (merge {:name   "bank"
            :model  {:n 5 :total 50}
            :client (BankClient. (atom false) 5 10 nil)}
           opts)))
