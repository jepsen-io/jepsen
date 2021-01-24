(ns jecci.postgres.sequential
  (:require [jecci.utils.dbms.sequential :as s]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jecci.postgres.db :as db]
            [jepsen.client :as client]
            [jepsen.core :as jepsen]))

(def SequentialClientTranslation
  {
   :create-table (fn [t] [(str "create table if not exists " t
                            " (tkey varchar(255) primary key)")])
   :read-key (fn [table-id k]
               [(str "select tkey from "
                  table-id
                  " where tkey = ?") k])
   })

(defrecord pg-SequentialClient [dbclient]
  client/Client
  (open! [this test node]
    (pg-SequentialClient. (client/open! dbclient test node)))
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

(defn gen-SequentialClient [table-count tbl-created? conn]
  (pg-SequentialClient. (s/gen-SequentialClient table-count tbl-created? conn SequentialClientTranslation)))
