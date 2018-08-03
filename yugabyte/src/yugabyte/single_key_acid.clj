(ns yugabyte.single-key-acid
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [clojurewerkz.cassaforte [client :as cassandra]
                                     [query :refer :all]
                                     [policies :refer :all]
                                     [cql :as cql]]
            [yugabyte [core :refer :all]]
            )
  (:import (com.datastax.driver.core.exceptions UnavailableException
                                                WriteTimeoutException
                                                ReadTimeoutException
                                                NoHostAvailableException)))

(def table-name "single_key_acid")

(defrecord CQLSingleKey [conn]
  client/Client
  (open! [this test node]
    (info "Opening connection to " node)
    (assoc this :conn (cassandra/connect [node] {:protocol-version 3})))
  (setup! [this test]
    (locking setup-lock
      (cql/create-keyspace conn keyspace
                           (if-not-exists)
                           (with {:replication
                                  {"class" "SimpleStrategy"
                                   "replication_factor" 3}}))
      (cql/use-keyspace conn keyspace)
      (cql/create-table conn table-name
                        (if-not-exists)
                        (column-definitions {:id :int
                                             :val :int
                                             :primary-key [:id]}))
      (->CQLSingleKey conn)))
  (invoke! [this test op]
    (let [id   (key (:value op))
          val  (val (:value op))]
      (case (:f op)
      :write (try
               (cql/insert-with-ks conn keyspace table-name {:id id :val val})
               (assoc op :type :ok)
               (catch UnavailableException e
                 (assoc op :type :fail :error (.getMessage e)))
               (catch WriteTimeoutException e
                 (assoc op :type :info :error :timed-out))
               (catch NoHostAvailableException e
                 (info "All nodes are down - sleeping 2s")
                 (Thread/sleep 2000)
                 (assoc op :type :fail :error (.getMessage e))))
      :cas (try
             (let [[expected-val new-val] val
                   res (cql/update-with-ks conn keyspace table-name {:val new-val}
                                   (only-if [[= :val expected-val]]) (where [[= :id id]]))
                   applied (get (first res) (keyword "[applied]"))
                   ]
               (assoc op :type (if applied :ok :fail)))
             (catch UnavailableException e
               (assoc op :type :fail :error (.getMessage e)))
             (catch WriteTimeoutException e
               (assoc op :type :info :error :timed-out))
             (catch NoHostAvailableException e
               (info "All nodes are down - sleeping 2s")
               (Thread/sleep 2000)
               (assoc op :type :fail :error (.getMessage e))))
      :read (try (wait-for-recovery 30 conn)
                 (let [value (->> (cql/select-with-ks conn keyspace table-name (where [[= :id id]])) first :val)]
                   (assoc op :type :ok :value (independent/tuple id value)))
                 (catch UnavailableException e
                   (info "Not enough replicas - failing")
                   (assoc op :type :fail :error (.getMessage e)))
                 (catch ReadTimeoutException e
                   (assoc op :type :fail :error :timed-out))
                 (catch NoHostAvailableException e
                   (info "All nodes are down - sleeping 2s")
                   (Thread/sleep 2000)
                   (assoc op :type :fail :error (.getMessage e)))))))
  (teardown! [this test])
  (close! [this test]
    (info "Closing client with conn" conn)
    (cassandra/disconnect! conn)))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn test
  [opts]
  (yugabyte-test
    (merge opts
           {:name "Single key ACID"
            :client (CQLSingleKey. nil)
            :concurrency (max 10 (:concurrency opts))
            :client-generator (independent/concurrent-generator
                               10
                               (range)
                               (fn [k]
                                 (->> (gen/reserve 5 (gen/mix [w cas cas]) r)
                                      (gen/delay-til 1/2)
                                      (gen/stagger 0.1)
                                      (gen/limit 100))))
            :model (model/cas-register 0)
            :checker (checker/compose {:perf (checker/perf)
                                       :indep (independent/checker
                                         (checker/compose
                                          {:timeline (timeline/html)
                                           :linear   (checker/linearizable)}))})
            })))
