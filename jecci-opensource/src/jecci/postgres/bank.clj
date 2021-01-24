(ns jecci.postgres.bank
  (:require [jecci.utils.dbms.bank :as b]
            [jecci.postgres.db :as db]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client]
            [jepsen.core :as jepsen]))


(def BankClientTranslation
  {:create-table ["create table if not exists accounts
                  (id int not null primary key,
                  balance bigint not null)"]
   :read-all [(str "select * from accounts")]
   :read-from (fn [read-lock from]
                [(str "select * from accounts where id=? " read-lock) from])
   :read-to (fn [read-lock to]
              [(str "select * from accounts where id = ? " read-lock) to])
   :update-from (fn [amount from]
                  ["update accounts set balance = balance - ? where id = ?" amount from])
   :update-to (fn [amount to]
                ["update accounts set balance = balance + ? where id = ?" amount to])})


(defrecord pg-BankClient [dbclient]
  client/Client
  (open! [this test node]
    (pg-BankClient. (client/open! dbclient test node)))
  (setup! [this test]
    (when (db/isleader? (:node# (:conn dbclient)))
      (client/setup! dbclient test))
    (jepsen/synchronize test))
  (invoke! [this test op]
    (if (or (db/isleader? (:node# (:conn dbclient)))
            (= (:f op) :read))
      (client/invoke! dbclient test op)
      (throw (Exception. "not writing to backup"))))
  (teardown! [this test]
    (client/teardown! dbclient test))
  (close! [this test]
    (client/close! dbclient test)))

(defn gen-BankClient [conn]
  (pg-BankClient. (b/gen-BankClient conn BankClientTranslation)))


(def MultiBankClientTranslation
  {:create-table (fn [a] [(str "create table if not exists accounts" a
                               "(id     int not null primary key,"
                               "balance bigint not null)")])
   :read-balance (fn [x] [(str "select balance from accounts"
                               x)])
   :read-from (fn [read-lock from]
                [(str "select balance from " from " " (:read-lock test))])
   :read-to (fn [read-lock to]
              [(str "select balance from " to " " (:read-lock test))])
   :update-from (fn [amount from]
                  [(str "update " from " set balance = balance - ? where id = 0") amount])
   :update-to (fn [amount to]
                [(str "update " to " set balance = balance + ? where id = 0") amount])})

(defrecord pg-MultiBankClient [multibankclient]
  client/Client
  (open! [this test node]
    (pg-MultiBankClient. (client/open! multibankclient test node)))
  (setup! [this test]
    (when (db/isleader? (:node# (:conn multibankclient)))
      (client/setup! multibankclient test))
    (jepsen/synchronize test))
  (invoke! [this test op]
    (if (or (db/isleader? (:node# (:conn multibankclient)))
            (= (:f op) :read))
      (client/invoke! multibankclient test op)
      (assoc op :type :fail, :value (info (:node# (:conn multibankclient)) "not writing to backup"))))
  (teardown! [this test]
    (client/teardown! multibankclient test))
  (close! [this test]
    (client/close! multibankclient test)))

(defn gen-MultiBankClient [conn tbl-created?]
  (pg-MultiBankClient. (b/gen-MultiBankClient conn tbl-created? MultiBankClientTranslation)))


