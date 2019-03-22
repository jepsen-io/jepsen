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
                    [util :as util]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.bank :as bank]
            [jepsen.faunadb [client :as f]
                            [query :as q]]
            [dom-top.core :as dt]
            [clojure.core.reducers :as r]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as cstr]
            [clojure.tools.logging :refer :all]
            [knossos.op :as op]))

(def accounts-name "accounts")
(def accounts (q/class accounts-name))

(def idx-name "all_accounts")
(def idx (q/index idx-name))

(defmacro wrapped-query
  [op & exprs]
  `(f/with-errors ~op #{:read}
     (try
       ~@exprs
       (catch com.faunadb.client.errors.BadRequestException e#
         (if (= (.getMessage e#) "transaction aborted: balance would go negative")
           (assoc ~op :type :fail, :error :negative)
           (throw e#))))))

(defn create-class!
  "Creates the class for accounts"
  [test conn]
  (f/upsert-class! conn {:name accounts-name}))

(defn create-accounts!
  "Creates the initial accounts for a test"
  [test conn]
  ; And initial account
  (f/query conn (q/let [ref (q/ref accounts (first (:accounts test)))]
                  (q/when (q/not (q/exists? ref))
                    (q/create ref {:data {:balance (:total-amount test)}}))))

  (when (:fixed-instances test)
    ; Create remaining accounts up front
    (f/query conn
             (q/do*
               (map (fn [acct]
                      (let [acct (q/ref accounts acct)]
                        (f/upsert-by-ref acct {:data {:balance 0}})))
                    (rest (:accounts test))))))

  (when (:at-query test)
    ; We're going to introduce 10 seconds of jitter here, so let's wait 10
    ; seconds to make sure we don't query before the beginning of time
    (Thread/sleep 10000)))

(defrecord BankClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (f/with-retry
      (create-class! test conn)
      (create-accounts! test conn)))

  (invoke! [this test op]
    (case (:f op)
      :read
      (wrapped-query
        op
        (let [[ts res] (f/query
                         conn
                         (f/maybe-at test conn
                                     (mapv
                                       (fn [i]
                                         (let [acct (q/ref accounts i)]
                                           (q/when (q/exists? acct)
                                             [i (q/select ["data" "balance"]
                                                          (q/get acct))])))
                                       (:accounts test))))
              ; Convert read array of [acct, bal] pairs to a map.
              res (->> res
                       (remove nil?)
                       (map vec)
                       (into {}))]
          (assoc op :type :ok, :value res, :ts (str ts))))

      :transfer
      (wrapped-query op
        (let [{:keys [from to amount]} (:value op)]
          (f/query
            conn
            (q/do
              (q/let [a (q/- (q/if (q/exists? (q/ref accounts from))
                               (q/select ["data" "balance"]
                                         (q/get (q/ref accounts from)))
                               0)
                             amount)]
                (q/cond
                  (q/< a 0) (q/abort "balance would go negative")

                  ; If we're using fixed instances, write 0 instead of deleting
                  (q/and (q/= a 0) (not (:fixed-instances test)))
                  (q/delete (q/ref accounts from))

                  (q/update
                    (q/ref accounts from)
                    {:data {:balance a}})))

              (q/if (q/exists? (q/ref accounts to))
                (q/let [b (q/+ (q/select ["data" "balance"]
                                         (q/get (q/ref accounts to)))
                               amount)]
                  (q/update (q/ref accounts to)
                            {:data {:balance b}}))
                (q/create (q/ref accounts to)
                          {:data {:balance amount}}))))
          (assoc op :type :ok)))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

; Like BankClient, but performs reads using an index instead.
(defrecord IndexClient [bank-client conn]
  client/Client
  (open! [this test node]
    (let [b (client/open! bank-client test node)]
      (assoc this :bank-client b :conn (:conn b))))

  (setup! [this test]
    (f/with-retry
      (create-class! test conn)
      (f/upsert-index! conn {:name idx-name
                             :source accounts
                             :active true
                             :serialized (boolean (:serialized-indices test))
                             :values [{:field ["ref"]}
                                      {:field ["data" "balance"]}]})
      (f/wait-for-index conn idx)
      (create-accounts! test conn)))

  (invoke! [this test op]
    (if (= :read (:f op))
      (wrapped-query op
        (->> (f/query-all conn (q/match idx))
             (map (fn [[ref balance]] [(Long/parseLong (:id ref)) balance]))
             (into (sorted-map))
             (assoc op :type :ok, :value)))

      (client/invoke! bank-client test op)))


  (teardown! [this test]
    (client/teardown! bank-client test))

  (close! [this test]
    (client/close! bank-client test)))

(defn workload
  "A basic bank workload"
  [opts]
  (-> (bank/test)
      (assoc :client (BankClient. nil))
      (update :generator (partial gen/delay 1/10))))

(defn index-workload
  "A version which uses indices for reads"
  [opts]
  (-> (bank/test)
      (assoc :client (IndexClient. (BankClient. nil) nil))
      (update :generator (partial gen/delay 1/10))))
