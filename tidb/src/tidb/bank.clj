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
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]
            [clojure.tools.logging :refer :all]))

(defrecord BankClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    ; sigh, tidb falls over if it gets more than a handful of contended
    ; requests per second; let's try to make its life easier
    (locking BankClient
      (c/with-conn-failure-retry conn
        (c/execute! conn ["create table if not exists accounts
                          (id     int not null primary key,
                          balance bigint not null)"])
        (doseq [a (:accounts test)]
          (try
            (with-txn-retries conn
              (c/insert! conn :accounts {:id      a
                                         :balance (if (= a (first (:accounts test)))
                                                    (:total-amount test)
                                                    0)}))
            (catch java.sql.SQLIntegrityConstraintViolationException e nil))))))

  (invoke! [this test op]
    (with-txn op [c conn]
      (try
        (case (:f op)
          :read (->> (c/query c [(str "select * from accounts")])
                     (map (juxt :id :balance))
                     (into (sorted-map))
                     (assoc op :type :ok, :value))

          :transfer
          (let [{:keys [from to amount]} (:value op)
                b1 (-> c
                       (c/query [(str "select * from accounts where id = ? "
                                      (:read-lock test)) from]
                                {:row-fn :balance})
                       first
                       (- amount))
                b2 (-> c
                       (c/query [(str "select * from accounts where id = ? "
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
                    (do (c/execute! c ["update accounts set balance = balance - ? where id = ?" amount from])
                        (c/execute! c ["update accounts set balance = balance + ? where id = ?" amount to])
                        (assoc op :type :ok))
                    (do (c/update! c :accounts {:balance b1} ["id = ?" from])
                        (c/update! c :accounts {:balance b2} ["id = ?" to])
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
        (with-txn-retries conn
          (c/with-conn-failure-retry conn
            (doseq [a (:accounts test)]
              (info "Creating table accounts" a)
              (c/execute! conn [(str "create table if not exists accounts" a
                                     "(id     int not null primary key,"
                                     "balance bigint not null)")])
              (try
                (info "Populating account" a)
                (c/insert! conn (str "accounts" a)
                           {:id 0
                            :balance (if (= a (first (:accounts test)))
                                       (:total-amount test)
                                       0)})
                (catch java.sql.SQLIntegrityConstraintViolationException e
                  nil))))))))

  (invoke! [this test op]
    (with-txn op [c conn]
      (try
        (case (:f op)
          :read
          (->> (:accounts test)
               (map (fn [x]
                      [x (->> (c/query c [(str "select balance from accounts"
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
                       (c/query
                        [(str "select balance from " from
                              " " (:read-lock test))]
                        {:row-fn :balance})
                       first
                       (- amount))
                b2 (-> c
                       (c/query [(str "select balance from " to
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
                    (do (c/execute! c [(str "update " from " set balance = balance - ? where id = 0") amount])
                        (c/execute! c [(str "update " to " set balance = balance + ? where id = 0") amount])
                        (assoc op :type :ok))
                    (do (c/update! c from {:balance b1} ["id = 0"])
                        (c/update! c to {:balance b2} ["id = 0"])
                        (assoc op :type :ok)))))))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn multitable-workload
  [opts]
  (assoc (workload opts)
         :client (MultiBankClient. nil (atom false))))
