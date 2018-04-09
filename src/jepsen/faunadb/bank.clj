(ns jepsen.faunadb.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
  (:use jepsen.faunadb.query)
  (:import com.faunadb.client.errors.UnavailableException)
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:import com.faunadb.client.types.Result)
  (:import com.faunadb.client.types.Value)
  (:import com.google.common.collect.ImmutableList)
  (:import java.io.IOException)
  (:import java.util.concurrent.ExecutionException)
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [core :as jepsen]
                    [fauna :as fauna]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.faunadb.client :as f]
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
      (Result/success
        {:ref (. (getField value 0 Codec/REF) getId)
         :balance (getField value 1 Codec/LONG)}))

    (encode [this v]
      (Value/from (ImmutableList/of
                    (:ref v) (:balance v))))))

(def BalancesField
  "A field extractor for balances"
  (.collect (Field/at (into-array String ["data"])) (Field/as BalancesCodec)))

(defn do-index-read
  [conn]
  (f/queryGet
    conn
    (Map
      (Paginate (Match idxRef))
      (Lambda
        (v "r")
        (Arr
          (Var "r")
          (Select balancePath (Get (Var "r"))))))
    BalancesField))

(defn await-replication
  [conn n]
  (let [iCnt (count (do-index-read conn))]
    (if (not= n iCnt)
      (do
        (Thread/sleep 1000)
        (await-replication conn n)))))

(defmacro wrapped-query
  [op & exprs]
  `(try
    ~@exprs
    (catch ExecutionException e#
      (cond
        (instance? UnavailableException (.getCause e#))
        (assoc ~op :type :fail, :error [:unavailable (.. e# (getCause) (getMessage))])

        (instance? IOException (.getCause e#))
        (assoc ~op :type :fail, :error [:io (.. e# (getCause) (getMessage))])

        (= (.. e# (getCause) (getMessage)) "transaction aborted: balance would go negative")
        (assoc ~op :type :fail, :error :negative)

        :else (throw e#)))))

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
              "source" classRef)))

        (info (cstr/join ["Creating " n " accounts"]))
        (f/query
          conn
          (Do
            (mapv
              (fn [i]
                (Create (Ref classRef i)
                        (Obj "data" (Obj "balance" (v starting-balance)))))
              (range n))))))
    (jepsen/synchronize test)
    (await-replication conn n))

  (invoke! [this test op]
    (case (:f op)
      :read
      (wrapped-query op
        (let [n (:value op)]
          (->>
            (f/queryGet
              conn
              (Obj "data" (Arr
                (mapv
                  (fn [i]
                    (Arr
                      (Ref classRef i)
                      (Select balancePath (Get (Ref classRef i)))))
                (range n))))
              BalancesField)
            (assoc op :type :ok, :value))))

      :index-read
      (wrapped-query op
        (->> (do-index-read conn)
          (assoc op :type :ok, :value)))

      :transfer
      (wrapped-query op
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
          (assoc op :type :ok)))))

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
  [model op]
  (let [balances (mapv :balance (:value op))]
    (cond (not= (:n model) (count balances))
          {:type     :wrong-n
           :expected (:n model)
           :found    (count balances)
           :op       op}

          (not= (:total model)
                (reduce + balances))
          {:type     :wrong-total
           :expected (:total model)
           :found    (reduce + balances)
           :op       op}

          (some neg? balances)
          {:type     :negative-value
           :found    balances
           :op       op})))

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [bad-reads (->> history
                        (r/filter op/ok?)
                        (r/filter #(not= :transfer (:f %)))
                        (r/map (fn [op] (balance-check model op)))
                        (r/filter identity)
                        (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defn bank-test-base
  [opts]
  (fauna/basic-test
    (merge
      {:client {:client (:client opts)
                :during (->> (gen/mix [bank-read bank-index-read bank-diff-transfer])
                          (gen/clients))
                :final (->> (gen/seq [(gen/once bank-read) (gen/once bank-index-read)])
                         (gen/clients))}
       :checker (checker/compose
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
