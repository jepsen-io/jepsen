(ns jepsen.faunadb.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
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
            [jepsen.faunadb [client :as f]
                            [query :as q]]
            [clojure.core.reducers :as r]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as cstr]
            [clojure.tools.logging :refer :all]
            [knossos.op :as op]))

(def accounts "accounts")
(def accounts* (q/class accounts))

(def idxRef
  "All Accounts index ref"
  (q/index "all_accounts"))

(def balancePath
  "Path to balance data"
  ["data" "balance"])

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

(pprint (macroexpand `(q/fn [r] [r (q/select balancePath (q/get r))])))

(defn do-index-read
  [conn]
  (f/queryGet
    conn
    (q/map
      (q/paginate (q/match idxRef))
      (q/fn [r]
        [r (q/select balancePath (q/get r))]))
    BalancesField))

(defn await-replication
  ; TODO: what is this doing exactly?
  [conn n]
  (let [iCnt (try (count (do-index-read conn))
                  (catch java.util.concurrent.ExecutionException e
                    (if (instance? com.faunadb.client.errors.UnavailableException e)
                      -1
                      (throw e))))]
    (if (not= n iCnt)
      (do
        (Thread/sleep 1000)
        (recur conn n)))))

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
        (f/query conn (q/create-class {:name "accounts"}))
        (f/query
          conn
          (q/create-index {:name "all_accounts"
                           :source accounts*}))

        (info "Creating" n "accounts")
        (f/query
          conn
          (apply q/do
            (mapv
              (fn [i]
                (q/create (q/ref accounts i)
                          {:data {:balance starting-balance}}))
              (range n))))))
    (jepsen/synchronize test)
    ; TODO: oh hellooooo
    (await-replication conn n))

  (invoke! [this test op]
    (case (:f op)
      :read
      (wrapped-query
        op
        (let [n (:value op)]
          (->> (f/queryGet
                 conn
                 {:data (mapv
                          (fn [i]
                            (let [acct (q/ref accounts i)]
                              [acct
                               (q/select balancePath (q/get acct))]))
                          (range n))}
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
            (q/do
              (q/let [a (q/-
                          (q/select balancePath
                            (q/get (q/ref accounts from)))
                          amount)]
                ; TODO: should this check both balances? why the or?
                (q/if (q/< a 0)
                  (q/abort "balance would go negative")
                  (q/update
                    (q/ref accounts from)
                    {:data {:balance a}})))
              (q/let [b (q/+ (q/select balancePath
                                       (q/get (q/ref accounts to)))
                             amount)]
                (q/update
                  (q/ref accounts to)
                  {:data {:balance b}}))))
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
