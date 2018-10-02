(ns jepsen.faunadb.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
  (:import com.faunadb.client.errors.UnavailableException)
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:import com.faunadb.client.types.Result)
  (:import com.faunadb.client.types.Value)
  (:import java.io.IOException)
  (:import java.util.concurrent.ExecutionException)
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [core :as jepsen]
                    [fauna :as fauna]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.bank :as bank]
            [jepsen.faunadb [client :as f]
                            [query :as q]]
            [clojure.core.reducers :as r]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as cstr]
            [clojure.tools.logging :refer :all]
            [knossos.op :as op]))

(def accounts-name "accounts")
(def accounts (q/class accounts-name))

(def idx-name "all_accounts")
(def idx (q/index idx-name))

(defn do-index-read
  [conn]
  ; TODO: figure out how to iterate over queries containing pagination
  (->> (f/query conn
                (q/map
                  (q/paginate (q/match idx))
                  (q/fn [r]
                    [r (q/select ["data" "balance"] (q/get r))])))
       :data
       (map (fn [[ref balance]]
                [(Long/parseLong (:id ref)) balance]))
       (into {})))

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

(defrecord BankClient [tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (f/query conn (q/create-class {:name "accounts"}))
        (info "Creating accounts" (:accounts test))
        (f/query
          conn
          (q/do (q/create (q/ref accounts (first (:accounts test)))
                          {:data {:balance (:total-amount test)}})
                (apply q/do
                       (mapv
                         (fn [acct]
                           (q/create (q/ref accounts acct)
                                     {:data {:balance 0}}))
                         (rest (:accounts test)))))))))

  (invoke! [this test op]
    (case (:f op)
      :read
      (wrapped-query
        op
        (->> (f/query conn
                      {:data (mapv
                               (fn [i]
                                 (let [acct (q/ref accounts i)]
                                   [acct
                                    (q/select ["data" "balance"]
                                              (q/get acct))]))
                               (:accounts test))})
             :data
             (map (fn [[ref balance]] [(Long/parseLong (:id ref)) balance]))
             (into {})
             (assoc op :type :ok, :value)))

      :transfer
      (wrapped-query op
        (let [{:keys [from to amount]} (:value op)]
          (f/query
            conn
            (q/do
              (q/let [a (q/- (q/select ["data" "balance"]
                                       (q/get (q/ref accounts from)))
                             amount)]
                (q/if (q/< a 0)
                  (q/abort "balance would go negative")
                  (q/update
                    (q/ref accounts from)
                    {:data {:balance a}})))
              (q/let [b (q/+ (q/select ["data" "balance"]
                                       (q/get (q/ref accounts to)))
                             amount)]
                (q/update
                  (q/ref accounts to)
                  {:data {:balance b}}))))
          (assoc op :type :ok)))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

; TODO: index reads variant
; We're not creating this index in the individual client because I want to avoid
; the possibility that index updates are introducing an unnecessary
; synchronization point.
        ;(f/query
        ;  conn
        ;  (q/create-index {:name "all_accounts"
        ;                   :source accounts}))

(defn bank-test-base
  [opts]
  (let [workload (bank/test)]
    (fauna/basic-test
      (merge
        (dissoc workload :generator)
        {:client {:client (:client opts)
                  :during (->> (:generator workload)
                               (gen/delay 1/10)
                               (gen/clients))
                  :final  (gen/clients nil)}
         :checker (checker/compose
                    {:perf     (checker/perf)
                     :timeline (timeline/html)
                     :details  (:checker workload)})}
        (dissoc opts :client)))))

(defn test
  [opts]
  (bank-test-base
    (merge {:name   "bank"
            :client (BankClient. (atom false) nil)}
           opts)))
