(ns yugabyte.ycql.bank
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.core.reducers :as r]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.tests.bank :as bank]
            [jepsen.checker.timeline :as timeline]
            [knossos.op :as op]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.query :as q :refer :all]
            [yugabyte.ycql.client :as c]))

(def setup-lock (Object.))
(def keyspace   "jepsen")
(def table-name "accounts")

(c/defclient CQLBank keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table-name
      (q/if-not-exists)
      (q/column-definitions {:id      :int
                             :balance :bigint
                             :primary-key [:id]}))
    (info "Creating accounts")
    (c/with-retry
      (cql/insert-with-ks conn keyspace table-name
                          {:id (first (:accounts test))
                           :balance (:total-amount test)})
      (doseq [a (rest (:accounts test))]
        (cql/insert conn table-name
                    {:id a, :balance 0}))))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :read
        (->> (cql/select-with-ks conn keyspace table-name)
             (map (juxt :id :balance))
             (into (sorted-map))
             (assoc op :type :ok, :value))

        :transfer
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
          (assoc op :type :ok)))))

  (teardown! [this test]))

;; Shouldn't be used until we support transactions with selects.
(c/defclient CQLMultiBank keyspace []
  (setup! [this test]
    (info "Creating accounts")
    (doseq [a (:accounts test)]
      (info "Creating table" a)
      (c/create-transactional-table
        conn (str table-name a)
        (q/if-not-exists)
        (q/column-definitions {:id          :int
                               :balance     :bigint
                               :primary-key [:id]}))

      (info "Populating account" a)
      (c/with-retry
        (cql/insert-with-ks conn keyspace (str table-name a)
                            {:id      a
                             :balance (if (= a (first (:accounts test)))
                                        (:total-amount test)
                                        0)}))))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :read
        (let [as (shuffle (:accounts test))]
          (->> as
               (mapv (fn [x]
                       ;; TODO - should be wrapped in a transaction after we
                       ;; support transactions with selects.
                       (->> (cql/select-with-ks conn keyspace
                                                (str table-name x)
                                                (where [[= :id x]]))
                            first
                            :balance)))
               (zipmap as)
               (assoc op :type :ok, :value)))

        :transfer
        (let [{:keys [from to amount]} (:value op)]
          (cassandra/execute conn
                             (str "BEGIN TRANSACTION "
                                  (str "UPDATE " keyspace "." table-name from
                                       " SET balance = balance - " amount
                                       " WHERE id = " from ";")
                                  (str "UPDATE " keyspace "." table-name to
                                       " SET balance = balance + " amount
                                       " WHERE id = " to ";")
                                  "END TRANSACTION;"))
          (assoc op :type :ok)))))

  (teardown! [this test]))
