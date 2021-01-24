(ns jecci.utils.dbms.bank
  (:require [clojure.string :as str]
            [jepsen
             [client :as client]
             [generator :as gen]
             [checker :as checker]]
            [jepsen.tests.bank :as bank]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer :all]
            [jecci.utils.dbms.sql :as s]))


(defrecord BankClient [conn translation]
  client/Client
  (open! [this test node]
    (assoc this :conn (s/open node test)))

  (setup! [this test]
     (locking BankClient
      (s/with-conn-failure-retry conn
        (s/execute! conn (:create-table translation))
        (doseq [a (:accounts test)]
          (try
            (s/with-txn-retries conn
              (s/insert! conn :accounts {:id      a
                                         :balance (if (= a (first (:accounts test)))
                                                    (:total-amount test)
                                                    0)}))
            (catch java.sql.SQLIntegrityConstraintViolationException e nil))))))

  (invoke! [this test op]
    (s/with-txn op [c conn]
      (try
        (case (:f op)
          :read (->> (s/query c (:read-all translation))
                     (map (juxt :id :balance))
                     (into (sorted-map))
                     (assoc op :type :ok, :value))

          :transfer
          (let [{:keys [from to amount]} (:value op)
                b1 (-> c
                       (s/query ((:read-from translation) (:read-lock test) from)
                                {:row-fn :balance})
                       first
                       (- amount))
                b2 (-> c
                       (s/query ((:read-to translation) (:read-lock test) to)
                                {:row-fn :balance})
                       first
                       (+ amount))]
            (cond (neg? b1)
                  (assoc op :type :fail, :value [:negative from b1])
                  (neg? b2)
                  (assoc op :type :fail, :value [:negative to b2])
                  true
                  (if (:update-in-place test)
                    (do (s/execute! c ((:update-from translation) amount from))
                        (s/execute! c ((:update-to translation) amount to))
                        (assoc op :type :ok))
                    (do (s/update! c :accounts {:balance b1} ["id = ?" from])
                        (s/update! c :accounts {:balance b2} ["id = ?" to])
                        (assoc op :type :ok)))))))))

  (teardown! [_ test])

  (close! [_ test]
    (s/close! conn)))

(defn gen-BankClient [conn translation]
  (BankClient. conn translation))

(defrecord MultiBankClient [conn tbl-created? translation]
  client/Client
  (open! [this test node]
    (assoc this :conn (s/open node test)))
  (setup! [this test]
    (locking tbl-created?
    (when (compare-and-set! tbl-created? false true)
      (s/with-txn-retries conn
        (s/with-conn-failure-retry conn
          (doseq [a (:accounts test)]
            (info "Creating table accounts" a)
            (s/execute! conn ((:create-table translation) a))
            (try
              (info "Populating account" a)
              (s/insert! conn (str "accounts" a)
                {:id 0
                 :balance (if (= a (first (:accounts test)))
                            (:total-amount test)
                            0)})
              (catch java.sql.SQLIntegrityConstraintViolationException e
                nil)))))))	
    )
  (invoke! [this test op]
    (s/with-txn op [c conn]
     (try
       (case (:f op)
         :read
         (->> (:accounts test)
           (map (fn [x]
                  [x (->> (s/query c ((:read-balance translation) x)
                            {:row-fn :balance})
                       first)]))
           (into (sorted-map))
           (assoc op :type :ok, :value))

         :transfer
         (let [{:keys [from to amount]} (:value op)
               from (str "accounts" from)
               to   (str "accounts" to)
               b1 (-> c
                    (s/query
                      ((:read-from translation) (:read-lock test) from)
                      {:row-fn :balance})
                    first
                    (- amount))
               b2 (-> c
                    (s/query ((:read-to translation) (:read-lock test) to)
                      {:row-fn :balance})
                    first
                    (+ amount))]
           (cond (neg? b1)
                 (assoc op :type :fail, :error [:negative from b1])
                 (neg? b2)
                 (assoc op :type :fail, :error [:negative to b2])
                 true
                 (if (:update-in-place test)
                   (do (s/execute! c ((:update-from translation) amount from))
                       (s/execute! c ((:update-to translation) amount to))
                       (assoc op :type :ok))
                   (do (s/update! c from {:balance b1} ["id = 0"])
                       (s/update! c to {:balance b2} ["id = 0"])
                       (assoc op :type :ok)))))))))
  (teardown! [this test]

    )
  (close! [this test]
    (s/close! conn)
    )
  )

(defn gen-MultiBankClient [conn tbl-created? translation]
                     (MultiBankClient. conn tbl-created? translation))
