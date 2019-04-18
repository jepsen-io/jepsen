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
                               (if (:use-index test) "sk" "id") " = ?")
                          k])
                first
                :val)
         :w (do (c/execute! conn [(str "insert into " table
                                  " (id, sk, val) values (?, ?, ?)"
                                  " on duplicate key update val = ?")
                             k k v v])
                v))])

(defrecord TxnClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (c/execute! conn ["create table if not exists lf
                        (id  int not null primary key,
                         sk  int not null,
                         val int)"])
      (when (:use-index test)
        (c/create-index! conn ["create index lf_sk_val on lf (sk, val)"]))))

  (invoke! [this test op]
    (c/with-txn op [c conn]
      (assoc op
             :type  :ok
             :value (mapv (partial mop! c test "lf") (:value op)))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  [opts]
  (assoc (lf/workload 10)
         :client (TxnClient. nil)))
