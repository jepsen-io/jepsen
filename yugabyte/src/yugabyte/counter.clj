(ns yugabyte.counter
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             ]
            [jepsen.checker.timeline :as timeline]
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

(def table-name "counters")

(defrecord CQLCounterClient [conn]
  client/Client
  (open! [this test node]
    (info "Opening connection to " node)
    (assoc this :conn (cassandra/connect [node] {:protocol-version 3
                                                 :retry-policy (retry-policy :no-retry-on-client-timeout)})))
  (setup! [this test]
    (locking setup-lock
      (cql/create-keyspace conn keyspace
                           (if-not-exists)
                           (with {:replication
                                  {"class"              "SimpleStrategy"
                                   "replication_factor" 3}}))
      (cql/use-keyspace conn keyspace)
      (cql/create-table conn table-name
                        (if-not-exists)
                        (column-definitions {:id :int
                                             :count :counter
                                             :primary-key [:id]}))
      (cql/update conn table-name {:count (increment-by 0)}
                  (where [[= :id 0]]))))

  (invoke! [this test op]
    (case (:f op)
      :add (try (do
                  (cql/update-with-ks conn keyspace table-name
                              {:count (increment-by (:value op))}
                              (where [[= :id 0]]))
                  (assoc op :type :ok))
                (catch UnavailableException e
                  (assoc op :type :fail :error (.getMessage e)))
                (catch WriteTimeoutException e
                  (assoc op :type :info :value :write-timed-out))
                (catch OperationTimedOutException e
                  (assoc op :type :info :error :client-timed-out))
                (catch NoHostAvailableException e
                  (info "All the servers are down - waiting 2s")
                  (Thread/sleep 2000)
                  (assoc op :type :fail :error (.getMessage e)))
                (catch DriverException e
                  (if (re-find #"Value write after transaction start|Conflicts with higher priority transaction|Conflicts with committed transaction|Operation expired: Failed UpdateTransaction.* status: COMMITTED .*: Transaction expired"
                               (.getMessage e))
                    ; Definitely failed
                    (assoc op :type :fail :error (.getMessage e))
                    (throw e))))

      :read (try (let [value (->> (cql/select-with-ks conn keyspace table-name
                                                      (where [[= :id 0]]))
                                  first
                                  :count)]
                   (assoc op :type :ok :value value))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :value (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :fail :value :read-timed-out))
                 (catch OperationTimedOutException e
                   (assoc op :type :fail :error :client-timed-out))
                 (catch NoHostAvailableException e
                   (info "All the servers are down - waiting 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :error (.getMessage e))))))

  (teardown! [this test])

  (close! [this test]
          (info "Closing client with conn" conn)
          (cassandra/disconnect! conn)))

(def add {:type :invoke :f :add :value 1})
(def sub {:type :invoke :f :add :value -1})
(def r {:type :invoke :f :read})

(defn test-inc
  [opts]
  (yugabyte-test
    (merge opts
           {:name             "cql-counter-inc"
            :client           (CQLCounterClient. nil)
            :client-generator (->>
                                (repeat 100 add)
                                (cons r)
                                gen/mix
                                (gen/delay 1/10))
            :model            nil
            :checker
            (checker/compose
              {:perf     (checker/perf)
               :timeline (timeline/html)
               :counter  (checker/counter)})})))

(defn test-inc-dec
  [opts]
  (yugabyte-test
    (merge opts
           {:name             "cql-counter-inc-dec"
            :client           (CQLCounterClient. nil)
            :client-generator (->>
                                (take 100 (cycle [add sub]))
                                (cons r)
                                gen/mix
                                (gen/delay 1/10))
            :model            nil
            :checker
                              (checker/compose
                                {:perf     (checker/perf)
                                 :timeline (timeline/html)
                                 :counter  (checker/counter)})})))
