(ns yugabyte.ysql.default-value
  "This test is designed to stress a specific part of YugaByte DB's
  non-transactional DDL to expose anomalies in DML.

  Note: Right now YB doesn't actually support adding columns with defaults.
  We'll create tables intead. The following is what we'd *like* to do.

  We simulate a migration in which a user wishes to add a column with a default
  value of `0` to an existing table, and execute concurrent inserts into the
  table, and concurrent reads, looking for cases where the column exists, but
  its value is `null` instead."
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

(def table "foo")

(defn insert!
  "Inserts a row into a table."
  [conn table]
  (c/execute! conn [(str "insert into " table " (dummy) values (?)") 1]))

(defn read-ordered
  "Reads every value in table ordered by k."
  [conn table]
  (let [res (c/query conn [(str "select k, v from " table " order by k")])]
    (mapv :v res)))

(defn read-natural
  "Reads every value in table using natural ordering."
  [conn table]
  (->> (c/query conn [(str "select (v) from " table)])))

(defn create-table!
  "Creates a table for the given relation. Swallows already-exists errors,
  because YB can't do `create ... if not exists` properly."
  [conn table]
  (try
    (c/execute! conn (j/create-table-ddl table
                                         [[:dummy :int]
                                          [:v :int :default "0"]]
                                         {:conditional? true}))
    (catch org.postgresql.util.PSQLException e
      (when-not (re-find #"already exists" (.getMessage e))
        (throw e)))))

(defn drop-table!
  "Drops the given table."
  [conn table]
  (c/execute! conn (j/drop-table-ddl table {:conditional? true})))

(defn add-column!
  "Adds an integer column with a default to the given table."
  [conn table column default-value]
  (c/execute! conn [(str "alter table " table " add " column " int default "
                         default-value)]))

(defn drop-column!
  "Removes the given column"
  [conn table column]
  (c/execute! conn [(str "alter table " table " drop column " column)]))

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
          ; (info e# "with-table caught")
          (throw e#)))))

(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper])

  (invoke-op! [this test op c conn-wrapper]
    (try
      (case (:f op)
        :read   (assoc op :type :ok, :value (read-natural c table))
        :insert (do (insert! c table)
                    (assoc op :type :ok))
        :create-table (do (create-table! c table)
                          (assoc op :type :ok))
        :drop-table   (do (drop-table! c table)
                          (assoc op :type :ok))
        :add-column (do (add-column! c table (:value op) 1)
                        (assoc op :type :ok))
        :drop-column (do (drop-column! c table (:value op))
                         (assoc op :type :ok)))
      (catch org.postgresql.util.PSQLException e
        (if (re-find #"column .+ does not exist" (.getMessage e))
          (assoc op :type :fail, :error :column-does-not-exist)
          (throw e))))))

(c/defclient Client InternalClient)
