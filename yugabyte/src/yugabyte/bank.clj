(ns yugabyte.bank
  "Simulates transfers between bank accounts"
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.core.reducers :as r]
            [jepsen [client    :as client]
             [checker   :as checker]
             [generator :as gen]
            ]
            [jepsen.checker.timeline :as timeline]
            [knossos.op :as op]
            [clojurewerkz.cassaforte [client :as cassandra]
             [query :refer :all]
             [policies :refer :all]
             [cql :as cql]]
            [yugabyte.core :refer :all]
            )
  (:import (com.datastax.driver.core.exceptions DriverException
                                                UnavailableException
                                                OperationTimedOutException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                NoHostAvailableException)))

(def table-name "accounts")

(defrecord CQLBank [n starting-balance conn]
  client/Client
  (open! [this test node]
    (info "Opening connection to " node)
    (assoc this :conn (cassandra/connect [node] {:protocol-version 3
                                                 :retry-policy (retry-policy :no-retry-on-client-timeout)})))
  (setup! [this test]
    (locking setup-lock
      (cql/create-keyspace conn keyspace
                           (if-not-exists)
                           (with
                             {:replication
                              {"class"              "SimpleStrategy"
                               "replication_factor" 3}}))
      (info "Creating table")
      (cassandra/execute conn (str "CREATE TABLE IF NOT EXISTS " keyspace "." table-name
                                   " (id INT PRIMARY KEY, balance BIGINT)"
                                   " WITH transactions = { 'enabled' : true }"))
      (dotimes [i n]
        (info "Creating account" i)
        (cql/insert-with-ks conn keyspace table-name {:id i :balance starting-balance}))))

  (invoke! [this test op]
    (case (:f op)
      :read
      (try
        (->> (cql/select-with-ks conn keyspace table-name)
             (mapv :balance)
             (assoc op :type :ok, :value))
        (catch UnavailableException e
          (info "Not enough replicas - failing")
          (assoc op :type :fail :error (.getMessage e)))
        (catch ReadTimeoutException e
          (assoc op :type :fail :error :read-timed-out))
        (catch OperationTimedOutException e
          (assoc op :type :fail :error :client-timed-out))
        (catch NoHostAvailableException e
          (info "All nodes are down - sleeping 2s")
          (Thread/sleep 2000)
          (assoc op :type :fail :error (.getMessage e))))

      :transfer
      (let [{:keys [from to amount]} (:value op)]
        (try
          (cassandra/execute conn
            (str "BEGIN TRANSACTION "
              (str "UPDATE " keyspace "." table-name " SET balance = balance - " amount " WHERE id = " from ";")
              (str "UPDATE " keyspace "." table-name " SET balance = balance + " amount " WHERE id = " to ";")
              "END TRANSACTION;"))
          (assoc op :type :ok)
          (catch UnavailableException e
            (assoc op :type :fail :error (.getMessage e)))
          (catch WriteTimeoutException e
            (assoc op :type :info :error :write-timed-out))
          (catch OperationTimedOutException e
            (assoc op :type :info :error :client-timed-out))
          (catch NoHostAvailableException e
            (info "All nodes are down - sleeping 2s")
            (Thread/sleep 2000)
            (assoc op :type :fail :error (.getMessage e)))
          (catch DriverException e
            (if (re-find #"Value write after transaction start|Conflicts with higher priority transaction|Conflicts with committed transaction|Operation expired: Failed UpdateTransaction.* status: COMMITTED .*: Transaction expired"
                         (.getMessage e))
              ; Definitely failed
              (assoc op :type :fail :error (.getMessage e))
              (throw e)))))))

  (teardown! [this test])

  (close! [this test]
    (info "Closing client with conn" conn)
    (cassandra/disconnect! conn)))

(defn bank-read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [test process]
  (let [n (-> test :client :n)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-int n)
             :to     (rand-int n)
             :amount (+ 1 (rand-int 5))}}))

(def bank-diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              bank-transfer))

(defn bank-checker
  "Balances must all be non-negative (not supported for now) and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [bad-reads (->> history
                           (r/filter op/ok?)
                           (r/filter #(= :read (:f %)))
                           (r/map (fn [op]
                                    (let [balances (:value op)]
                                      (cond (not= (:n model) (count balances))
                                            {:type :wrong-n
                                             :expected (:n model)
                                             :found    (count balances)
                                             :op       op}

                                            (not= (:total model)
                                                  (reduce + balances))
                                            {:type :wrong-total
                                             :expected (:total model)
                                             :found    (reduce + balances)
                                             :op       op}

                                            ; TODO: uncomment once we can support this test with non-negative balances.
                                            ;(some neg? balances)
                                            ;{:type     :negative-value
                                            ; :found    balances
                                            ; :op       op}
                                            ))))
                           (r/filter identity)
                           (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defn bank-test-base
  [opts]
  (yugabyte-test
    (merge {:client-generator (gen/mix [bank-read bank-diff-transfer])
            :client-final-generator (gen/once bank-read)
            :checker          (checker/compose
                                {:perf     (checker/perf)
                                 :timeline (timeline/html)
                                 :details  (bank-checker)})}
           opts)))

(defn test
  [opts]
  (bank-test-base
    (merge {:name   "cql-bank"
            :model  {:n 5 :total 50}
            :client (CQLBank. 5 10 nil)}
           opts)))

;; Shouldn't be used until we support transactions with selects.
(defrecord CQLMultiBank [n starting-balance conn]
  client/Client
  (open! [this test node]
    (info "Opening connection to " node)
    (assoc this :conn (cassandra/connect [node] {:protocol-version 3
                                                 :retry-policy (retry-policy :no-retry-on-client-timeout)})))
  (setup! [this test]
    (locking setup-lock
      (cql/create-keyspace conn keyspace
                           (if-not-exists)
                           (with
                             {:replication
                              {"class"              "SimpleStrategy"
                               "replication_factor" 3}}))
      (info "Creating accounts")
      (dotimes [i n]
               (info "Creating table" i)
               (cassandra/execute conn (str "CREATE TABLE IF NOT EXISTS " keyspace "." table-name i
                                            " (id INT PRIMARY KEY, balance BIGINT)"
                                            " WITH transactions = { 'enabled' : true }"))
               (info "Populating account" i)
               (cql/insert-with-ks conn keyspace (str table-name i) {:id i :balance starting-balance}))))

  (invoke! [this test op]
    (case (:f op)
      :read
      (try
      (->> (range n)
        (mapv (fn [x]
          ;; TODO - should be wrapped in a transaction after we support transactions with selects.
          (->> (cql/select-with-ks conn keyspace (str table-name x) (where [[= :id x]]))
               first
               :balance)))
        (assoc op :type :ok, :value))
      (catch UnavailableException e
        (info "Not enough replicas - failing")
        (assoc op :type :fail :error (.getMessage e)))
      (catch ReadTimeoutException e
        (assoc op :type :fail :error :read-timed-out))
      (catch OperationTimedOutException e
        (assoc op :type :fail :error :client-timed-out))
      (catch NoHostAvailableException e
        (info "All nodes are down - sleeping 2s")
        (Thread/sleep 2000)
        (assoc op :type :fail :error (.getMessage e))))

      :transfer
      (let [{:keys [from to amount]} (:value op)]
      (try
        (cassandra/execute conn
          (str "BEGIN TRANSACTION "
            (str "UPDATE " keyspace "." table-name from " SET balance = balance - " amount " WHERE id = " from ";")
            (str "UPDATE " keyspace "." table-name to " SET balance = balance + " amount " WHERE id = " to ";")
            "END TRANSACTION;"))
        (assoc op :type :ok)
        (catch UnavailableException e
          (assoc op :type :fail :error (.getMessage e)))
        (catch WriteTimeoutException e
          (assoc op :type :info :error :write-timed-out))
        (catch OperationTimedOutException e
          (assoc op :type :info :error :client-timed-out))
        (catch NoHostAvailableException e
          (info "All nodes are down - sleeping 2s")
          (Thread/sleep 2000)
          (assoc op :type :fail :error (.getMessage e)))
        (catch DriverException e
          (if (re-find #"Value write after transaction start|Conflicts with higher priority transaction|Conflicts with committed transaction|Operation expired: Failed UpdateTransaction.* status: COMMITTED .*: Transaction expired"
                       (.getMessage e))
          ; Definitely failed
          (assoc op :type :fail :error (.getMessage e))
          (throw e)))))))

  (teardown! [this test])

  (close! [this test]
    (info "Closing client with conn" conn)
      (cassandra/disconnect! conn)))

(defn multitable-test
      [opts]
      (bank-test-base
        (merge {:name   "cql-bank-multitable"
                :model  {:n 5 :total 50}
                ;; TODO: remove generators override to use default bank generators after we support transactions with
                ;; selects.
                :client-generator (gen/mix [bank-diff-transfer])
                :client-final-generator (gen/once bank-read)
                :client (CQLMultiBank. 5 10 nil)}
               opts)))
