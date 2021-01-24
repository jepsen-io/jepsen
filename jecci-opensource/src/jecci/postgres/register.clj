(ns jecci.postgres.register
  (:require [jecci.utils.dbms.register :as r]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jecci.postgres.db :as db]
            [jepsen.client :as client]
            [jepsen.core :as jepsen]))

(def AtomicClientTranslation
  {
   :create-table ["create table if not exists test
                  (id   int primary key,
                   sk   int,
                   val  int)"]
   :create-index ["create index test_sk_val on test (sk, val)"]
   :insert (fn [id val']
             [(str "insert into test (id, sk, val) "
                "values (?, ?, ?) "
                "on conflict (id) do update set "
                "val = ?")
              id id val' val'])
   :read (fn [use-index read-lock id]
           [(str "select (val) from test where "
              (if use-index "sk" "id") " = ? "
              read-lock)
            id])
   })

(defrecord pg-AtomicClient [dbclient]
  client/Client
  (open! [this test node]
    (pg-AtomicClient. (client/open! dbclient test node)))
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

(defn gen-AtomicClient [conn]
  (pg-AtomicClient. (r/gen-AtomicClient conn AtomicClientTranslation)))
