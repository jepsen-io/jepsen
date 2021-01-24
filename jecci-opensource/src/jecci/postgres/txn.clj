(ns jecci.postgres.txn
  (:require [jecci.utils.dbms.txn :as t]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jecci.postgres.db :as db]
            [jepsen.client :as client]
            [jepsen.core :as jepsen]))

(def mopTranslation
  {
   :read (fn [table key read-lock k]
           (let [stmt [(str "select (val) from " table " where "
                         key " = ? " read-lock) k]]
             (info stmt)
             stmt))
   :insert (fn [table k v]
             [(str "insert into " table
                " (id, sk, val) values (?, ?, ?)"
                " on conflict (id) do update set val = ?")
              k k v v])
   :insert-cat (fn [table k v]
                 [(str "insert into " table
                    " (id, sk, val) values (?, ?, ?)"
                    " on conflict (id) do update set val = CONCAT("
                    table ".val, ',', ?)")
                  k k (str v) (str v)])
   })

(def mop! (t/gen-mop! mopTranslation))

(def ClientTranslation
  {
   :create-table (fn [table-name val-type]
                   [(str "create table if not exists " table-name
                      " (id  int not null primary key,
                      sk  int not null,
                      val " val-type ")")])
   :create-index (fn [table-name]
                   [(str "create index " table-name "_sk_val"
                      " on " table-name " (sk, val)")])
   })

(defrecord pg-Client [dbclient]
  client/Client
  (open! [this test node]
    (pg-Client. (client/open! dbclient test node)))
  (setup! [this test]
    (when (db/isleader? (:node# (:conn dbclient)))
      (client/setup! dbclient test)) 
    (jepsen/synchronize test))
  (invoke! [this test op]
    (if (db/isleader? (:node# (:conn dbclient)))
      (client/invoke! dbclient test op) 
     (throw (Exception. "not operating backup")))
    )
  (teardown! [this test]
    (client/teardown! dbclient test))
  (close! [this test]
    (client/close! dbclient test))
  )

(defn gen-Client [conn val-type table-count]
  (pg-Client. (t/gen-Client conn val-type table-count mop! ClientTranslation)))
