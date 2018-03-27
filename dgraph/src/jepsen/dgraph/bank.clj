(ns jepsen.dgraph.bank
  "Implements a bank-account test, where we transfer amounts between a pool of
  accounts, and verify that reads always see a constant amount."
  (:require [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [disorderly with-retry]]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [generator :as generator]]
            [jepsen.tests.bank :as bank])
  (:import (io.dgraph TxnConflictException)))

(defn find-account
  "Finds an account by key. Returns an empty account when none exists."
  [t k]
  (-> (c/query t "{ q(func: eq(key, $key)) { uid key amount } }" {:key k})
      :q
      first
      (dissoc :type) ; Note that we need :type for new accounts, but don't
                     ; want to update it normally.
      (or {:key k
           :type "account"
           :amount 0})))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "key: int @index(int) .\n"
                               "type: string @index(exact) .\n"
                               "amount: int .\n"
                               ))
    (try
      (c/with-txn [t conn]
        (c/upsert! t :key
                   {:key    (first (:accounts test))
                    :type   "account"
                    :amount (:total-amount test)})
        (info (c/schema t)))
      (catch TxnConflictException e)))

  (invoke! [this test op]
    (c/with-conflict-as-fail op
      (c/with-txn [t conn]
        (case (:f op)
          :read (->> (c/query t (str "{ q(func: eq(type, $type)) {\n"
                                     "  key\n"
                                     "  amount\n"
                                     "}}")
                              {:type "account"})
                     :q
                     (map (juxt :key :amount))
                     (into (sorted-map))
                     (assoc op :type :ok, :value))
          :transfer (let [{:keys [from to amount]} (:value op)
                          [from to] (disorderly
                                      (find-account t from)
                                      (find-account t to))
                          _ (info :from (pr-str from))
                          _ (info :to   (pr-str to))
                          from' (update from :amount - amount)
                          to'   (update to   :amount + amount)]
                      (if (neg? (:amount from'))
                        (assoc op :type :fail, :error :insufficient-funds)
                        (do (disorderly
                              (if (zero? (:amount from'))
                                (c/delete! t (:uid from'))
                                (c/mutate! t from'))
                              (if (zero? (:amount to'))
                                (c/delete! t (:uid to'))
                                (c/mutate! t to')))
                            (assoc op :type :ok))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  (merge (bank/test)
         {:client (Client. nil)}))
