(ns jepsen.stolon.client
  "Helper functions for interacting with PostgreSQL clients."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [with-retry]]
            [jepsen [client :as client]
                    [util :as util]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]
            [wall.hack :as wh])
  (:import (java.sql Connection)))

(defn open
  "Opens a connection to the given node."
  [test node]
  (let [spec  {:dbtype    "postgresql"
               ;:dbname    "jepsen"
               :host      node
               :port      (:postgres-port     test)
               :user      (:postgres-user     test)
               :password  (:postgres-password test)
               :sslmode   (:postgres-sslmode test)}
        spec  (if-let [pt (:prepare-threshold test)]
                (assoc spec :prepareThreshold pt)
                spec)
        ds    (j/get-datasource spec)
        conn  (j/get-connection ds)]
    conn))

(defn set-transaction-isolation!
  "Sets the transaction isolation level on a connection. Returns conn."
  [conn level]
  (.setTransactionIsolation
    conn
    (case level
      :serializable     Connection/TRANSACTION_SERIALIZABLE
      :repeatable-read  Connection/TRANSACTION_REPEATABLE_READ
      :read-committed   Connection/TRANSACTION_READ_COMMITTED
      :read-uncommitted Connection/TRANSACTION_READ_UNCOMMITTED))
  conn)

(defn close!
  "Closes a connection."
  [^java.sql.Connection conn]
  (.close conn))

(defn await-open
  "Waits for a connection to node to become available, returning conn. Helpful
  for starting up."
  [node]
  (with-retry [tries 100]
    (info "Waiting for" node "to come online...")
    (let [conn (open node)]
      (try (j/execute-one! conn
                           ["create table if not exists jepsen_await ()"])
           conn
           (catch org.postgresql.util.PSQLException e
             (condp re-find (.getMessage e)
               ; Ah, good, someone else already created the table
               #"duplicate key value violates unique constraint \"pg_type_typname_nsp_index\""
               conn

               (throw e)))))
    (catch org.postgresql.util.PSQLException e
      (when (zero? tries)
        (throw e))

      (Thread/sleep 5000)
      (condp re-find (.getMessage e)
        #"connection attempt failed"
        (retry (dec tries))

        #"Connection to .+ refused"
        (retry (dec tries))

        #"An I/O error occurred"
        (retry (dec tries))

        (throw e)))))

(defmacro with-errors
  "Takes an operation and a body; evals body, turning known errors into :fail
  or :info ops."
  [op & body]
  `(try ~@body
        (catch clojure.lang.ExceptionInfo e#
          (warn e# "Caught ex-info")
          (assoc ~op :type :info, :error [:ex-info (.getMessage e#)]))

        (catch org.postgresql.util.PSQLException e#
          (condp re-find (.getMessage e#)
            #"ERROR: could not serialize access"
            (assoc ~op :type :fail, :error [:could-not-serialize (.getMessage e#)])

            #"ERROR: deadlock detected"
            (assoc ~op :type :fail, :error [:deadlock (.getMessage e#)])

            #"An I/O error occurred"
            (assoc ~op :type :info, :error :io-error)

            #"connection has been closed"
            (assoc ~op :type :info, :error :connection-has-been-closed)

            (throw e#)))))
