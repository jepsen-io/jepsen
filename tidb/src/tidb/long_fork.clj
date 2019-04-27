(ns tidb.long-fork
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [generator :as gen]]
            [jepsen.tests.long-fork :as lf]
            [tidb.sql :as c :refer :all]))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn test table [f k v]]
  [f k (case f
         :r (-> conn
                (c/query [(str "select (val) from " table " where "
                               (if (:use-index test) "sk" "id") " = ? "
                               (:read-lock test))
                          k])
                first
                :val)

         :w (do (c/execute! conn [(str "insert into " table
                                  " (id, sk, val) values (?, ?, ?)"
                                  " on duplicate key update val = ?")
                             k k v v])
                v)

         :append
         (let [r (c/execute!
                   conn
                   [(str "insert into " table " (id, sk, val) values (?, ?, ?)"
                         " on duplicate key update val = CONCAT(val, ',', ?)")
                    k k (str v) (str v)])]
           v))])

(defrecord TxnClient [conn val-type]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (c/execute! conn [(str "create table if not exists lf
                             (id  int not null primary key,
                              sk  int not null,
                              val " val-type ")")])
      (when (:use-index test)
        (c/create-index! conn ["create index lf_sk_val on lf (sk, val)"]))))

  (invoke! [this test op]
    (let [txn       (:value op)
          use-txn?  (< 1 (count txn))
          ;use-txn?  false
          txn'  (if use-txn?
                  (c/with-txn op [c conn]
                    (mapv (partial mop! c test "lf") txn))
                  ; If there's 1 or 0 elements in the txn, we don't need a
                  ; transactional scope to execute it.
                  (mapv (partial mop! conn test "lf") txn))]
      (assoc op :type :ok, :value txn', :txn? use-txn?)))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  [opts]
  (assoc (lf/workload 10)
         :client (TxnClient. nil "int")))
