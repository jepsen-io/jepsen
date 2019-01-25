(ns yugabyte.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.core.reducers :as r]
            [jepsen [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]]
            [jepsen.tests.bank :as bank]
            [jepsen.checker.timeline :as timeline]
            [knossos.op :as op]
            [clojurewerkz.cassaforte [client :as cassandra]
             [query :refer :all]
             [policies :refer :all]
             [cql :as cql]]
            [yugabyte [core :refer :all]
                      [auto :as auto]])
  (:import (com.datastax.driver.core.exceptions DriverException
                                                UnavailableException
                                                OperationTimedOutException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                NoHostAvailableException
                                                TransportException)))

(def setup-lock (Object.))
(def keyspace   "jepsen")
(def table-name "accounts")

(defmacro with-errors
  "Takes an op, a set of idempotent operation :fs, and a body. Evalates body,
  and catches common errors, returning an appropriate completion for `op`."
  [op idempotent & body]
  `(let [crash# (if (~idempotent (:f ~op)) :fail :info)]
     (try
       ~@body
       (catch UnavailableException e#
         ; I think this was used back when we blocked on all nodes being online
         ; (info "Not enough replicas - failing")
         (assoc ~op :type :fail, :error [:unavailable (.getMessage e#)]))

       (catch ReadTimeoutException e#
         (assoc ~op :type crash#, :error :read-timed-out))

       (catch OperationTimedOutException e#
         (assoc ~op :type crash#, :error :operation-timed-out))

       (catch TransportException e#
         (condp re-find (.getMessage e#)
           #"Connection has been closed"
           (assoc ~op :type crash#, :error :connection-closed)

           (throw e#)))

       (catch NoHostAvailableException e#
         (info "All nodes are down - sleeping 2s")
         (Thread/sleep 2000)
         (assoc ~op :type :fail :error [:no-host-available (.getMessage e#)]))

       (catch DriverException e#
         (if (re-find #"Value write after transaction start|Conflicts with higher priority transaction|Conflicts with committed transaction|Operation expired: Failed UpdateTransaction.* status: COMMITTED .*: Transaction expired"
                      (.getMessage e#))
           ; Definitely failed
           (assoc ~op :type :fail, :error (.getMessage e#))
           (throw e#))))))

(defrecord CQLBank [conn]
  client/Client
  (open! [this test node]
    (info "Opening connection to" node)
    (let [c (cassandra/connect
              [node]
              {:protocol-version  3
               :retry-policy      (retry-policy :no-retry-on-client-timeout)})]
      (assoc this :conn c)))

  (setup! [this test]
    ; This is a workaround for a bug in Yugabyte's create-table code
    (locking setup-lock
      (cql/create-keyspace conn keyspace
                           (if-not-exists)
                           (with
                             {:replication
                              {"class"              "SimpleStrategy"
                               "replication_factor" 3}}))
      (info "Creating table")
      (cassandra/execute conn (str "CREATE TABLE IF NOT EXISTS "
                                   keyspace "." table-name
                                   " (id INT PRIMARY KEY, balance BIGINT)"
                                   " WITH transactions = { 'enabled' : true }"))

      (info "Creating accounts")
      (cql/insert-with-ks conn keyspace table-name
                          {:id (first (:accounts test))
                           :balance (:total-amount test)})
      (doseq [a (rest (:accounts test))]
        (cql/insert-with-ks conn keyspace table-name
                            {:id a, :balance 0}))))

  (invoke! [this test op]
    (with-errors op #{:read}
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

  (teardown! [this test])

  (close! [this test]
    (info "Closing client with conn" conn)
    (cassandra/disconnect! conn)))

(defn bank-test-base
  [opts]
  (let [workload (bank/test {:negative-balances? true})]
    (yugabyte-test
      (merge (dissoc workload :generator)
             {:client-generator (:generator workload)
              :checker          (checker/compose
                                  {:perf     (checker/perf)
                                   ;:timeline (timeline/html)
                                   :details  (:checker workload)})}
             opts))))

(defn test
  [opts]
  (bank-test-base
    (merge {:name   "cql-bank"
            :client (CQLBank. nil)}
           opts)))

;; Shouldn't be used until we support transactions with selects.
(defrecord CQLMultiBank [conn]
  client/Client
  (open! [this test node]
    (info "Opening connection to " node)
    (let [c (cassandra/connect [node]
                               {:protocol-version 3
                                :retry-policy (retry-policy
                                                :no-retry-on-client-timeout)})]

      (assoc this :conn c)))

  (setup! [this test]
    (locking setup-lock
      (cql/create-keyspace conn keyspace
                           (if-not-exists)
                           (with
                             {:replication
                              {"class"              "SimpleStrategy"
                               "replication_factor" 3}}))
      (info "Creating accounts")
      (doseq [a (:accounts test)]
        (info "Creating table" a)
        (cassandra/execute conn (str "CREATE TABLE IF NOT EXISTS "
                                     keyspace "." table-name a
                                     " (id INT PRIMARY KEY, balance BIGINT)"
                                     " WITH transactions = { 'enabled' : true }"))
        (info "Populating account" a)
        (cql/insert-with-ks conn keyspace (str table-name a)
                            {:id      a
                             :balance (if (= a (first (:accounts test)))
                                        (:total-amount test)
                                        0)}))))

  (invoke! [this test op]
    (with-errors op #{:read}
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

  (teardown! [this test])

  (close! [this test]
    (info "Closing client with conn" conn)
      (cassandra/disconnect! conn)))

(defn multitable-test
  [opts]
  (bank-test-base
    (merge {:name   "cql-bank-multitable"
            :client (CQLMultiBank. nil)}
           opts)))
