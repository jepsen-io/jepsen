(ns jecci.postgres.monotonic
  (:require [jecci.utils.dbms.monotonic :as m]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jecci.postgres.db :as db]
            [jepsen.client :as client]
            [jepsen.core :as jepsen]))

(def IncrementClientTranslation
  {
   :create-table ["create table if not exists cycle
                  (pk  int not null primary key,
                   sk  int not null,
                   val int)"]
   :create-index ["create index cycle_sk_val on cycle (sk, val)"]
   :update-cycle (fn [k] [(str "update cycle set val = val + 1"
                            " where pk = ?") k])
   })

(defrecord pg-IncrementClient [dbclient]
  client/Client
  (open! [this test node]
    (pg-IncrementClient. (client/open! dbclient test node)))
  (setup! [this test]
    (when (db/isleader? (:node# (:conn dbclient)))
      (client/setup! dbclient test)) 
    (jepsen/synchronize test))
  (invoke! [this test op]
    (if (or (db/isleader? (:node# (:conn dbclient)))
         (= (:f op) :read))
      (client/invoke! dbclient test op) 
     (throw (Exception. "not writing to backup")))
    )
  (teardown! [this test]
    (client/teardown! dbclient test))
  (close! [this test]
    (client/close! dbclient test))
  )

(defn gen-IncrementClient [conn]
  (pg-IncrementClient. (m/gen-IncrementClient conn IncrementClientTranslation)))
