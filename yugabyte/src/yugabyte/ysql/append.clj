(ns yugabyte.ysql.append
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]
            [yugabyte.ysql.client :as c]))

(defn table-count
  "How many tables do we need for a test?"
  [test]
  (:table-count test 5))

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "append" i))

(defn table-for
  "What table should we use for the given key?"
  [test k]
  (table-name (mod (hash k) (table-count test))))

(def keys-per-row 2)

(defn row-for
  "What row should we use for the given key?"
  [test k]
  (quot k keys-per-row))

(defn col-for
  "What column should we use for the given key?"
  [test k]
  (str "v" (mod k keys-per-row)))

(defn read-primary
  "Reads a key based on primary key"
  [conn table row col]
  (some-> conn
          (c/query [(str "select (" col ") from " table " where k = ?") row])
          first
          (get (keyword col))
          (str/split #",")
          (->> ; Append might generate a leading , if the row already exists
               (remove str/blank?)
               (mapv #(Long/parseLong %)))))

(defn append-primary!
  "Writes a key based on primary key."
  [conn table row col v]
  (let [r (c/execute! conn [(str "update " table
                                 " set " col " = CONCAT(" col ", ',', ?) "
                                 "where k = ?") v row])]
    (when (= [0] r)
      ; No rows updated
      (c/execute! conn
                  [(str "insert into " table
                        " (k, k2, " col ") values (?, ?, ?)") row row v]))
    v))

(defn read-secondary
  "Reads a key based on a predicate over a secondary key, k2"
  [conn table row col]
  (some-> conn
          (c/query [(str "select (" col ") from " table
                         " where (k2 * 2) - ? = ?")
                    col col])
          first
          (get (keyword col))
          (str/split #",")
          (->> (mapv #(Long/parseLong %)))))

(defn append-secondary!
  "Writes a key based on a predicate over a secondary key, k2. Returns v."
  [conn table row col v]
  (let [r (c/execute! conn [(str "update " table
                                 " set " col " = CONCAT(" col ", ',', ?) "
                                 "where (k2 * 2) - ? = ?") v row row])]
    (when (= [0] r)
      ; No rows updated
      (c/execute! conn
                  [(str "insert into " table
                        "(k, k2, " col ") values (?, ?, ?)") row row v]))
    v))

(defn mop!
  "Executes a transactional micro-op of the form [f k v] on a connection, where
  f is either :r for read or :append for list append. Returns the completed
  micro-op."
  [conn test [f k v]]
  (let [table (table-for test k)
        row   (row-for test k)
        col   (col-for test k)]
    [f k (case f
           :r       (read-primary     conn table row col)
           :append  (append-primary!  conn table row col v))]))

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (->> (range (table-count test))
         (map table-name)
         (map (fn [table]
                (info "Creating table" table)
                (c/execute! c (j/create-table-ddl
                                table
                                (into
                                  [;[:k :int "unique"]
                                   [:k :int "PRIMARY KEY"]
                                   [:k2 :int]]
                                  ; Columns for n values packed in this row
                                  (map (fn [i] [(col-for test i) :text])
                                       (range keys-per-row)))
                                {:conditional? true}))))
         dorun))

  (invoke-op! [this test op c conn-wrapper]
    (let [txn       (:value op)
          use-txn?  (< 1 (count txn))
          ; use-txn?  false ; Just for making sure the checker actually works
          txn'      (if use-txn?
                      (c/with-txn c
                        (mapv (partial mop! c test) txn))
                      (mapv (partial mop! c test) txn))]
      (assoc op :type :ok, :value txn'))))

(c/defclient Client InternalClient)
