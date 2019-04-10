(ns tidb.long-fork
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [generator :as gen]]
            [jepsen.tests.long-fork :as lf]
            [tidb.sql :as c :refer :all]))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn table [f k v]]
  [f k (case f
         :r (-> conn
                (j/query [(str "select (val) from " table " where id = ?")
                          k])
                first
                :val)
         :w (do (j/execute! conn [(str "insert into " table
                                  " (id, val) values (?, ?)"
                                  " on duplicate key update val = ?")
                             k v v])
                v))])

(defrecord TxnClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (j/execute! conn ["create table if not exists lf
                        (id  int not null primary key,
                         val int)"])))

  (invoke! [this test op]
    (c/with-txn op [c conn]
      (assoc op
             :type  :ok
             :value (mapv (partial mop! conn "lf") (:value op)))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  [opts]
  (assoc (lf/workload 10)
         :client (TxnClient. nil)))
