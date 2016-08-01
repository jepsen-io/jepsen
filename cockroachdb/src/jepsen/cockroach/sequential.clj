(ns jepsen.cockroach.sequential
  "A sequential consistency test.

  Verify that client order is consistent with DB order by performing queries
  (in four distinct transactions) like

  A: insert x
  A: insert y
  B: read y
  B: read x

  A's process order enforces that x must be visible before y, so we should
  always read both or neither.

  Splits keys up onto different tables to make sure they fall in different
  shard ranges"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as c]
             [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh]]]
            [jepsen.cockroach.nemesis :as cln]
            [clojure.java.jdbc :as j]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op]))

(def table-prefix "String prepended to all table names." "seq_")

(defn table-names
  "Names of all tables"
  [table-count]
  (map (partial str table-prefix) (range table-count)))

(defn key->table
  "Turns a key into a table id"
  [table-count k]
  (str table-prefix (mod (hash k) table-count)))

(defn subkeys
  "The subkeys used for a given key, in order."
  [key-count k]
  (mapv (partial str k "_") (range key-count)))

(defmacro with-errors
  "We're doing multiple transactions per block here and want exceptions to
  prevent subsequent txns from running."
  [op [c conn] & body]
  `(c/with-annotate-errors
     (c/with-reconnect ~conn true
       (c/with-error-handling ~op
         ~@body))))

(defrecord Client [table-count key-count table-created? conn]
  client/Client

  (setup! [this test node]
    (Thread/sleep 2000)
    (let [conn (c/init-conn node)]
      (locking table-created?
        (when (compare-and-set! table-created? false true)
          (c/with-txn-notimeout {} [c conn]
            (info "Creating tables" (pr-str (table-names table-count)))
            (try
              (doseq [t (table-names table-count)]
                (j/execute! @conn [(str "drop table if exists " t)])
                (j/execute! @conn [(str "create table " t " (key varchar(255))")])
                (info "Created table" t))
              (catch java.sql.BatchUpdateException e
                (info e "caught")
                (info (.getNextException e) "next")
                (throw e))))))

      (assoc this :conn conn)))

  (invoke! [this test op]
    (with-errors op [c conn]
      (let [ks (subkeys key-count (:value op))]
        (case (:f op)
          :write (do (doseq [k ks]
                       (j/insert! @conn (key->table table-count k) {:key k})))
          (assoc op :type :ok))
        :read (->> ks
                   reverse
                   (mapv (fn [k]
                           (first
                             (j/query @conn [(str "select key from "
                                                  (key->table table-count k)
                                                  " where key = ?") k]
                                      :row-fn :key))))
                   (vector (:value op))
                   (assoc op :type :ok, :value)))))

  (teardown! [this test]
    (try
      (doseq [t (table-names table-count)]
        (c/with-timeout conn nil
          (j/execute! @conn ["drop table ?" t])))
      (finally
        (c/close-conn @conn)))))

(defn writes
  "We emit sequential integer keys for writes, logging the most recent n keys
  in the given atom, wrapping a PersistentQueue."
  [last-written]
  (let [k (atom -1)]
    (reify gen/Generator
      (op [this test process]
        (let [k (swap! k inc)]
          (swap! last-written #(-> % pop (conj k)))
          {:type :invoke, :f :write, :value k})))))

(defn reads
  "We use the last-written atom to perform a read of a randomly selected
  recently written value."
  [last-written]
  (gen/filter (comp complement nil? :value)
    (reify gen/Generator
      (op [this test process]
        {:type :invoke, :f :read, :value (rand-nth @last-written)}))))

(defn gen
  "Basic generator with n writers, and a buffer of 2n"
  [n]
  (let [last-written (atom
                       (reduce conj clojure.lang.PersistentQueue/EMPTY
                               (repeat (* 2 n) nil)))]
    (gen/reserve n (writes last-written)
                 (reads last-written))))

(defn test
  [opts]
  (let [gen (gen 5)]
  (c/basic-test
    (merge
      {:name "sequential"
       :concurrency c/concurrency-factor
       :client {:client (Client. 10 2 (atom false) nil)
                :during (gen/limit 10 (gen/stagger 1 gen))
                :final  (gen/once (gen/filter #(= :read (:f %)) gen))}
       :checker (checker/perf)}
      opts))))
