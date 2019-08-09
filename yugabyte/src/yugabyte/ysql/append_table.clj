(ns yugabyte.ysql.append-table
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system.

  Lists are encoded as rows in a table; key names are table names, and the set
  of all rows determines the list contents."
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

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "append" i))

(defn insert!
  "Inserts a row with value v into a table, returning v."
  [conn table v]
  (c/execute! conn [(str "insert into " table " (v) values (?)") v])
  v)

(defn create-table!
  "Creates a table for the given relation. Swallows already-exists errors,
  because YB can't do `create ... if not exists` properly."
  [conn table-name]
  (try
    (c/execute! conn (j/create-table-ddl table-name
                                         [[:k :SERIAL]
                                          [:v :int]]
                                         {:conditional? true}))
    (catch org.postgresql.util.PSQLException e
      (when-not (re-find #"already exists" (.getMessage e))
        (throw e)))))

(defn catch-dne
  "Returns a form for catching a relation-does-not-exist exception of the given
  class. Binds the name of the missing relation to `table`, and evaluates
  body."
  [class table & body]
  `(~'catch ~class e#
     (if-let [~table (nth (re-find #"relation \"(.+?)\" does not exist"
                                   (.getMessage e#)) 1)]
       (do ~@body)
       (throw e#))))

(defmacro with-table
  "Evaluates body, catching \"relation does not exist\" exceptions, and
  evaluating retry, then body again, if that occurs."
  [conn & body]
  (let [table-sym (gensym 'table)]
    `(try (do ~@body)
          ~(apply catch-dne 'java.sql.BatchUpdateException table-sym
                  `(info "Creating table" ~table-sym "and retrying")
                  `(create-table! ~conn ~table-sym)
                  body)
          ~(apply catch-dne 'org.postgresql.util.PSQLException table-sym
                  `(info "Creating table" ~table-sym "and retrying")
                  `(create-table! ~conn ~table-sym)
                  body)
        (catch Exception e#
          (info e# "with-table caught")
          (throw e#)))))

(defn mop!
  "Executes a transactional micro-op of the form [f k v] on a connection, where
  f is either :r for read or :append for list append. Returns the completed
  micro-op."
  [conn test [f k v]]
  (let [table (table-name k)]
      [f k (case f
             :r (->> (c/query conn [(str "select (v) from " table " order by k")])
                     (mapv :v))
             :append (insert! conn table v))]))

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper])

  (invoke-op! [this test op c conn-wrapper]
    (with-table c
      (let [txn       (:value op)
            use-txn?  (< 1 (count txn))
            ; use-txn?  false ; Just for making sure the checker actually works
            txn'      (if use-txn?
                        (c/with-txn c
                          (mapv (partial mop! c test) txn))
                        (mapv (partial mop! c test) txn))]
        (assoc op :type :ok, :value txn')))))

(c/defclient Client InternalClient)
