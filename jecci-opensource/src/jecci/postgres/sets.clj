(ns jecci.postgres.sets
  (:require [jecci.utils.dbms.sets :as s]
            [clojure.tools.logging :refer [info]]
            [clojure.string :as str]
            [jecci.postgres.db :as db]
            [jepsen.client :as client]
            [jepsen.core :as jepsen]))

(def SetClientTranslation
  {
   :create-table ["create table if not exists sets
                    (id   serial  not null primary key,
                    value  bigint not null)"]
   :read-all ["select * from sets"]
   })

(defrecord pg-SetClient [dbclient]
  client/Client
  (open! [this test node]
    (pg-SetClient. (client/open! dbclient test node)))
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


(defn gen-SetClient [conn]
  (pg-SetClient. (s/gen-SetClient conn SetClientTranslation)))

(def CasSetClientTranslation
  {
   :create-table ["create table if not exists sets
                  (id     int not null primary key,
                  value   text)"]
   :read-value (fn [read-lock] [(str "select (value) from sets"
                                  " where id = 0 "
                                  read-lock)])
   :update-value (fn [v e]
                   ["update sets set value = ? where id = 0"
                    (str v "," e)])
   :select-0 ["select (value) from sets where id = 0"]
   })

(defrecord pg-CasSetClient [dbclient]
  client/Client
  (open! [this test node]
    (pg-CasSetClient. (client/open! dbclient test node)))
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

(defn gen-CasSetClient [conn]
  (pg-CasSetClient. (s/gen-CasSetClient conn CasSetClientTranslation)))

