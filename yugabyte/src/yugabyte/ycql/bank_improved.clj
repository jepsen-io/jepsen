(ns yugabyte.ycql.bank-improved
  "Improved bank scenario

  Works only with transfer-without-deletes generator due to missing
  possibility of transactional reads in case of bank workload"
  (:refer-clojure :exclude
                  [test])
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.query :as q :refer :all]
            [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table-name "accounts")

(c/defclient CQLBankImproved keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table-name
      (q/if-not-exists)
      (q/column-definitions
        {:id          :int
         :balance     :bigint
         :primary-key [:id]}))
    (info "Creating accounts")
    (c/with-retry
      (cql/insert-with-ks conn keyspace table-name
                          {:id      (first (:accounts test))
                           :balance (:total-amount test)})
      (doseq [a (rest (:accounts test))]
        (cql/insert conn table-name
                    {:id a, :balance 0}))))

  (invoke! [this test op]
    (c/with-errors
      op #{:read}
      (case (:f op)
        :read
        (->> (cql/select-with-ks conn keyspace table-name)
             (map (juxt :id :balance))
             (into (sorted-map))
             (assoc op :type :ok, :value))

        :update
        (let [{:keys [from to amount]} (:value op)]
          (cassandra/execute
            conn
            ; TODO: separate reads from updates?
            (str "BEGIN TRANSACTION "
                 "UPDATE " keyspace "." table-name
                 " SET balance = balance - " amount " WHERE id = " from ";"

                 "UPDATE " keyspace "." table-name
                 " SET balance = balance + " amount " WHERE id = " to ";"
                 "END TRANSACTION;"))
          (assoc op :type :ok))

        :insert
        (let [{:keys [from to amount]} (:value op)]
          (cassandra/execute
            conn
            (str "BEGIN TRANSACTION "
                 "INSERT INTO " keyspace "." table-name
                 " (id, balance) VALUES (" to "," amount ");"

                 "UPDATE " keyspace "." table-name
                 " SET balance = balance - " amount " WHERE id = " from ";"
                 "END TRANSACTION;"))
          (assoc op :type :ok)))))

  (teardown! [this test]))
