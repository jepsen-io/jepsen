(ns tidb.txn
  "Client for transactional workloads."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [generator :as gen]]
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

(defrecord Client [conn val-type]
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
    (let [txn      (:value op)
          use-txn? (< 1 (count txn))]
          ;use-txn? false]
          (if use-txn?
            (c/with-txn op [c conn]
              (assoc op :type :ok, :value
                     (mapv (partial mop! c test "lf") txn)))
            (c/with-txn-aborts op
              (assoc op :type :ok, :value
                     (mapv (partial mop! conn test "lf") txn))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn client
  "Constructs a transactional client. Opts are:

    :val-type   An SQL type string, like \"int\", for the :val field schema."
  [opts]
  (Client. nil (:val-type opts "int")))
