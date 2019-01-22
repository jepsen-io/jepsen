(ns yugabyte.multi-key-acid
  (:require [clojure.tools.logging :refer [debug info warn]]
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
            [yugabyte.core :refer :all]
            )
  (:import (knossos.model Model)
           (com.datastax.driver.core.exceptions DriverException
                                                UnavailableException
                                                OperationTimedOutException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                NoHostAvailableException)))

(def table-name "multi_key_acid")

(defrecord MultiRegister []
  Model
  (step [this op]
        (assert (= (:f op) :txn))
        (try
        ; `knossos.model.memo/memo` function passing operation invocations as steps to the model, so in our case we can
        ; receive operation which value is simply `:read` without any sequence and need to handle that properly by
        ; returning current state.
        (if (= (:value op) :read) this
        (reduce
          (fn [state [f k v]]
            ; Apply this particular op
            (case f
              :read  (if (or (nil? v)
                             (= v (get state k)))
                       state
                       (reduced
                        (model/inconsistent
                         (str k ": " (pr-str (get state k)) "≠" (pr-str v)))))
              :write (assoc state k v)))
          this
          (:value op))))

(defn multi-register
  "A register supporting read and write transactions over registers identified
  by keys. Takes a map of initial keys to values. Supports a single :f for ops,
  :txn, whose value is a transaction: a sequence of [f k v] tuples, where :f is
  :read or :write, k is a key, and v is a value. Nil reads are always legal."
  [values]
  (map->MultiRegister values))

(defrecord CQLMultiKey [conn]
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
            (cassandra/execute conn (str "CREATE TABLE IF NOT EXISTS " keyspace "." table-name
                                         " (id INT PRIMARY KEY, val INT) WITH transactions = { 'enabled' : true }"))
            (->CQLMultiKey conn)))
  (invoke! [this test op]
           (assert (= (:f op) :txn))
           (let [value (:value op)]
             (if (= value :read)
               (try (wait-for-recovery 30 conn)
                 (assoc op :type :ok :value
                                 (->> (cql/select-with-ks conn keyspace table-name)
                                      (map (fn [x] [:read (:id x) (:val x)]))
                                      (sort-by #(second %))))
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
               (try
                 ; TODO - temporarily replaced by single DB call until YugaByteDB supports transaction spanning
                 ; multiple DB calls.
;                 (cassandra/execute conn "BEGIN TRANSACTION")
;                 (doseq [[sub-op id val] value]
;                   (assert (= sub-op :write))
;                   (cql/insert conn table-name {:id id :val val}))
;                 (cassandra/execute conn "END TRANSACTION;")
                 (cassandra/execute conn
                                    (str "BEGIN TRANSACTION "
                                         (->>
                                          (for [[sub-op id val] value]
                                            (do
                                              (assert (= sub-op :write))
                                              (str "INSERT INTO " keyspace "." table-name " (id, val) VALUES ("
                                                   id ", " val ");")))
                                          clojure.string/join)
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

(defn r [_ _] {:type :invoke, :f :txn :value :read})
(defn w [_ _]
  (let [val (rand-int 10)]
    {:type  :invoke,
     :f     :txn
     :value [[:write (rand-int 10) val] [:write (rand-int 10) val]]}))

(defn test
  [opts]
  (yugabyte-test
   (let [concurrency (max 10 (:concurrency opts))]
     (merge opts
            {:name             "Multi key ACID"
             :client           (CQLMultiKey. nil)
             :concurrency      concurrency
             :client-generator (->>
                                (gen/reserve (quot concurrency 2) r w)
                                (gen/delay-til 1/2)
                                (gen/stagger 0.1)
                                (gen/limit 100))
             :client-final-generator (gen/once r)
             :model            (multi-register {})
             :checker
             (checker/compose
              {:perf     (checker/perf)
               :timeline (timeline/html)
               :linear   (checker/linearizable)})}))))
