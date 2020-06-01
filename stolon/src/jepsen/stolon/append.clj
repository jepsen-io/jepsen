(ns jepsen.stolon.append
  "Test for transactional list append."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [elle.core :as elle]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]
             [util :as util :refer [parse-long]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.stolon [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]))

(def default-table-count 3)

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn test [f k v]]
  (let [table-count (:table-count test default-table-count)
        table (table-for table-count k)]
    [f k (case f
           :r (let [r (j/execute! conn
                                  [(str "select (val) from " table " where "
                                        (if (or (:use-index test)
                                                (:predicate-read test))
                                          "sk"
                                          "id")
                                        " = ? ")
                                   k]
                                  {:builder-fn rs/as-unqualified-lower-maps})]
                (when-let [v (:val (first r))]
                  (mapv parse-long (str/split v #","))))


           :w (do (j/execute! conn [(str "insert into " table
                                         " (id, sk, val) values (?, ?, ?)"
                                         ; " on duplicate key update val = ?")
                                         "on conflict do update set val = ?")
                                    k k v v])
                  v)

           :append
           (let [r (j/execute!
                     conn
                     [(str "insert into " table " as t"
                           " (id, sk, val) values (?, ?, ?)"
                           " on conflict (id) do update set"
                           " val = CONCAT(t.val, ',', ?) where t.id = ?")
                      k k (str v) (str v) k])]
             v))]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (let [c (c/await-open node)]
      (assoc this :conn c)))

  (setup! [_ test]
    (dotimes [i (:table-count test default-table-count)]
      ; OK, so first worrying thing: why can this throw duplicate key errors if
      ; it's executed with "if not exists"?
      (try
        (j/execute! conn
                    [(str "create table if not exists " (table-name i)
                          " (id int not null primary key,
                          sk int not null,
                          val text)")])
        (catch org.postgresql.util.PSQLException e
          (condp re-find (.getMessage e)
            #"duplicate key value violates unique constraint"
            :dup

            (throw e))))))

  (invoke! [_ test op]
    (c/with-errors op
      (let [txn       (:value op)
            use-txn?  (< 1 (count txn))
            txn'      (if use-txn?
                      ;(if true
                        (j/with-transaction [t conn {:isolation :serializable}]
                        ;(j/with-transaction [t conn {:isolation :read-committed}]
                          (mapv (partial mop! t test) txn))
                        (mapv (partial mop! conn test) txn))]
        (assoc op :type :ok, :value txn'))))

  (teardown! [_ test]
    (j/execute-one! conn ["drop table if exists lists"]))

  (close! [this test]
    (c/close! conn)))

(defn workload
  "A list append workload."
  [opts]
  (-> (append/test (assoc (select-keys opts [:key-count
                                             :max-txn-length
                                             :max-writes-per-key])
                          :min-txn-length 1
                          :consistency-models [:strict-serializable]))
      (assoc :client (Client. nil))))
