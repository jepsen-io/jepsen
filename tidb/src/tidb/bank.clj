(ns tidb.bank
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen
             [client :as client]
             [generator :as gen]
             [checker :as checker]]
            [jepsen.tests.bank :as bank]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [clojure.java.jdbc :as j]
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]
            [clojure.tools.logging :refer :all]))

(defrecord BankClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (j/execute! conn ["create table if not exists accounts
                      (id     int not null primary key,
                      balance bigint not null)"])
    (doseq [a (:accounts test)]
      (try
        (with-txn-retries
          (j/insert! conn :accounts {:id      a
                                     :balance (if (= a (first (:accounts test)))
                                                (:total-amount test)
                                                0)}))
        (catch java.sql.SQLIntegrityConstraintViolationException e nil))))

  (invoke! [this test op]
    (with-txn op [c conn]
      (try
        (case (:f op)
          :read (->> (j/query c [(str "select * from accounts")])
                     (map (juxt :id :balance))
                     (into (sorted-map))
                     (assoc op :type :ok, :value))

          :transfer
          (let [{:keys [from to amount]} (:value op)
                b1 (-> c
                       (j/query [(str "select * from accounts where id = ? "
                                      (:read-lock test)) from]
                                {:row-fn :balance})
                       first
                       (- amount))
                b2 (-> c
                       (j/query [(str "select * from accounts where id = ? "
                                      (:read-lock test))
                                 to]
                                {:row-fn :balance})
                       first
                       (+ amount))]
            (cond (neg? b1)
                  (assoc op :type :fail, :value [:negative from b1])
                  (neg? b2)
                  (assoc op :type :fail, :value [:negative to b2])
                  true
                  (if (:update-in-place test)
                    (do (j/execute! c ["update accounts set balance = balance - ? where id = ?" amount from])
                        (j/execute! c ["update accounts set balance = balance + ? where id = ?" amount to])
                        (assoc op :type :ok))
                    (do (j/update! c :accounts {:balance b1} ["id = ?" from])
                        (j/update! c :accounts {:balance b2} ["id = ?" to])
                        (assoc op :type :ok)))))))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn workload
  [opts]
  (assoc (bank/test)
         :client (BankClient. nil)))

; One bank account per table
(defrecord MultiBankClient [conn tbl-created?]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (doseq [a (:accounts test)]
          (info "Creating table accounts" a)
          (j/execute! conn [(str "create table if not exists accounts" a
                                 "(id     int not null primary key,"
                                 "balance bigint not null)")])
          (try
            (info "Populating account" a)
            (with-txn-retries
              (j/insert! conn (str "accounts" a)
                         {:id 0
                          :balance (if (= a (first (:accounts test)))
                                     (:total-amount test)
                                     0)}))
            (catch java.sql.SQLIntegrityConstraintViolationException e nil))))))

  (invoke! [this test op]
    (with-txn op [c conn]
      (try
        (case (:f op)
          :read
          (->> (:accounts test)
               (map (fn [x]
                      [x (->> (j/query c [(str "select balance from accounts"
                                               x)]
                                       {:row-fn :balance})
                              first)]))
               (into (sorted-map))
               (assoc op :type :ok, :value))

          :transfer
          (let [{:keys [from to amount]} (:value op)
                from (str "accounts" from)
                to   (str "accounts" to)
                b1 (-> c
                       (j/query
                        [(str "select balance from " from
                              " " (:read-lock test))]
                        {:row-fn :balance})
                       first
                       (- amount))
                b2 (-> c
                       (j/query [(str "select balance from " to
                                      " " (:read-lock test))]
                                {:row-fn :balance})
                       first
                       (+ amount))]
            (cond (neg? b1)
                  (assoc op :type :fail, :error [:negative from b1])
                  (neg? b2)
                  (assoc op :type :fail, :error [:negative to b2])
                  true
                  (if (:update-in-place test)
                    (do (j/execute! c [(str "update " from " set balance = balance - ? where id = 0") amount])
                        (j/execute! c [(str "update " to " set balance = balance + ? where id = 0") amount])
                        (assoc op :type :ok))
                    (do (j/update! c from {:balance b1} ["id = 0"])
                        (j/update! c to {:balance b2} ["id = 0"])
                        (assoc op :type :ok)))))))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn multitable-workload
  [opts]
  (assoc (workload opts)
         :client (MultiBankClient. nil (atom false))))
