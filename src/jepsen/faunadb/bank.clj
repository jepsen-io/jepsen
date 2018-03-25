(ns jepsen.faunadb.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
  (:use jepsen.faunadb.query)
  (:import java.util.concurrent.ExecutionException)
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:import com.faunadb.client.types.Result)
  (:import com.faunadb.client.types.Value)
  (:import com.google.common.collect.ImmutableList)
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.faunadb.client :as f]
            [jepsen.fauna :as fauna]
            [clojure.core.reducers :as r]
            [clojure.string :as cstr]
            [clojure.tools.logging :refer :all]
            [knossos.op :as op]))

(def classRef
  "Accounts class ref"
  (ClassRef (v "accounts")))

(def idxRef
  "All Accounts index ref"
  (IndexRef (v "all_accounts")))

(def balancePath
  "Path to balance data"
  (Arr (v "data") (v "balance")))

(defn getField
  [value idx codec]
  (.get (.. value (at (into-array Integer/TYPE [idx])) (to codec))))

(def BalancesCodec
  (reify Codec
    (decode [this value]
      (let [ref (. (getField value 0 Codec/REF) getId)
            covered (getField value 1 Codec/LONG)
            balance (getField value 2 Codec/LONG)]
        (Result/success {:ref ref, :covered covered, :balance balance})))

    (encode [this v]
      (Value/from (ImmutableList/of
                    (:ref v) (:idxBalance v) (:balance v))))))

(def BalancesField
  "A field extractor for balances"
  (.collect (Field/at (into-array String ["data"])) (Field/as BalancesCodec)))

(defrecord BankClient [tbl-created? n starting-balance conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (f/query conn (CreateClass (Obj "name" (v "accounts"))))
        (f/query
          conn
          (CreateIndex
            (Obj
              "name" (v "all_accounts")
              "source" classRef
              "serialized" (v true)
              "values" (Arr
                         (Obj "field" (Arr (v "ref")))
                         (Obj "field" (Arr (v "data") (v "balance")))))))

        (info (cstr/join ["Creating " n " accounts"]))
        (f/query
          conn
          (Do
            (mapv
              (fn [i]
                (Create (Ref classRef i)
                        (Obj "data" (Obj "balance" (v starting-balance)))))
              (range n)))))))

  (invoke! [this test op]
    (case (:f op)
      :read
      (let [n (:value op)]
        (->>
          (f/queryGet
            conn
            (Obj "data" (Arr
              (mapv
                (fn [i]
                  (Arr
                    (Ref classRef i)
                    (v starting-balance) ;stub out the covered value for this non-index read
                    (Select (Arr (v "data") (v "balance")) (Get (Ref classRef i)))))
              (range n))))
            BalancesField)
          (assoc op :type :ok, :value)))

      :index-read
      (let [n (:value op)]
        (->>
          (f/queryGet
            conn
            (Map
              (Paginate (Match idxRef))
              (Lambda
                (v "r")
                (Arr
                  (Select (v 0) (Var "r"))
                  (Select (v 1) (Var "r"))
                  (Select
                    (Arr (v "data") (v "balance"))
                    (Get (Select (v 0) (Var "r")))))))
            BalancesField)
          (assoc op :type :ok, :value)))

      :transfer
      (let [{:keys [from to amount]} (:value op)]
        (try
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
          (assoc op :type :ok)
        (catch ExecutionException e
          (if
            (= (.getMessage (.getCause e)) "transaction aborted: balance would go negative")
            (assoc op :type :fail, :error [:negative to])
            (throw e)))))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn bank-read
  "Reads the current state of all accounts without the index"
  [test _]
  (let [n (-> test :client :n)]
    {:type  :invoke,
     :f     :read,
     :value n}))

(defn bank-index-read
  "Reads the current state of all accounts through the index"
  [test _]
  (let [n (-> test :client :n)]
    {:type  :invoke,
     :f     :index-read,
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

(defn balance-check
  [model op typ field]
  (if
    (not= typ (:f op))
    nil
    (let [balances (mapv field (:value op))]
      (cond (not= (:n model) (count balances))
            {:type     :wrong-n
             :field    field
             :expected (:n model)
             :found    (count balances)
             :op       op}

            (not= (:total model)
                  (reduce + balances))
            {:type     :wrong-total
             :field    field
             :expected (:total model)
             :found    (reduce + balances)
             :op       op}

            (some neg? balances)
            {:type     :negative-value
             :field    field
             :found    balances
             :op       op}))))

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [bad-reads (->> history
                        (r/filter op/ok?)
                        (r/filter #(not= :transfer (:f %)))
                        (r/mapcat
                          (fn [op]
                              [(balance-check model op :read :balance)
                              (balance-check model op :index-read :covered)]))
                        (r/filter identity)
                        (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defn bank-test-base
  [opts]
  (fauna/basic-test
    (merge
      {:client      {:client (:client opts)
                     :during (->> (gen/mix [bank-read bank-index-read bank-diff-transfer])
                               (gen/clients))
                     :final (->> (gen/seq [(gen/once bank-read) (gen/once bank-index-read)])
                              (gen/clients))}
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
