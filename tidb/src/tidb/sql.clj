(ns tidb.sql
  (:require [clojure.string :as str]
            [jepsen [util :as util :refer [timeout]]]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def connect-timeout 10000)
(def socket-timeout  20000)
(def open-timeout
  "How long will we wait for an open call?"
  100000)

(defn conn-spec
  "jdbc connection spec for a node."
  [node]
  {:classname       "org.mariadb.jdbc.Driver"
   :subprotocol     "mariadb"
   :subname         (str "//" (name node) ":4000/test")
   :user            "root"
   :password        ""
   :connectTimeout  connect-timeout
   :socketTimeout   socket-timeout})

(defn init-conn!
  "Sets initial variables on a connection, based on test options. Options are:

  :auto-retry   - If true, automatically retries transactions.

  Returns conn."
  [conn test]
  (j/execute! conn ["set @@tidb_disable_txn_auto_retry = ?"
                    (if (:auto-retry test) 0 1)])
  conn)

(defn open
  "Opens a connection to the given node."
  [node test]
  (timeout open-timeout
           (throw+ {:type :connect-timed-out
                    :node node})
           (util/retry 1
                       (try
                         (let [spec   (conn-spec node)
                               conn   (j/get-connection spec)
                               spec'  (j/add-connection spec conn)]
                           (assert spec')
                           (init-conn! spec' test))
                         (catch java.sql.SQLNonTransientConnectionException e
                           ; Conn refused
                           (throw e))
                         (catch Throwable t
                           (info t "Unexpected connection error, retrying")
                           (throw t))))))

(defn close!
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (j/db-find-connection conn)]
    (.close c))
  (dissoc conn :connection))

(def await-id
  "Used to generate unique identifiers for awaiting cluster stabilization"
  (atom 0))

(defn await-node
  "Waits for a node to become ready by opening a connection, creating a table,
  and inserting a record."
  [node]
  (info "Waiting for" node)
  (if (let [c (open node {})]
        (try
          (j/execute! c ["create table if not exists jepsen_await
                         (id int primary key, val int)"])
          (j/insert! c "jepsen_await" {:id  (swap! await-id inc)
                                       :val (rand-int 5)})
          true
          (finally
            (close! c))))
    (info node "ready")
    (recur node)))

(def rollback-msg
  "mariadb drivers have a few exception classes that use this message"
  "Deadlock found when trying to get lock; try restarting transaction")

(defmacro capture-txn-abort
  "Converts aborted transactions to an ::abort keyword"
  [& body]
  `(try ~@body
        (catch java.sql.SQLTransactionRollbackException e#
          (if (= (.getMessage e#) rollback-msg)
            ::abort
            (throw e#)))
        (catch java.sql.BatchUpdateException e#
          (if (= (.getMessage e#) rollback-msg)
            ::abort
            (throw e#)))
        (catch java.sql.SQLException e#
          (if (re-find #"can not retry select for update statement" (.getMessage e#))
            ::abort
            (throw e#)))))

(defmacro with-txn-retries
  "Retries body on rollbacks."
  [& body]
  `(loop []
     (let [res# (capture-txn-abort ~@body)]
       (if (= ::abort res#)
         (recur)
         res#))))

(defmacro with-txn-aborts
  "Aborts body on rollbacks."
  [op & body]
  `(let [res# (capture-txn-abort ~@body)]
     (if (= ::abort res#)
       (assoc ~op :type :fail)
       res#)))

(defmacro with-error-handling
  "Common error handling for errors"
  [op & body]
  `(try ~@body
        (catch java.sql.SQLNonTransientConnectionException e#
          (condp = (.getMessage e#)
            "WSREP has not yet prepared node for application use"
            (assoc ~op :type :fail, :value (.getMessage e#))
            (throw e#)))))

(defmacro with-txn
  "Executes body in a transaction, with a timeout, automatically retrying
  conflicts and handling common errors."
  [op [c conn] & body]
  `(timeout (+ 1000 socket-timeout) (assoc ~op :type :info, :value :timed-out)
            (with-error-handling ~op
              (with-txn-retries
                ; PingCAP says that the default isolation level for
                ; transactions is snapshot isolation
                ; (https://github.com/pingcap/docs/blob/master/sql/transaction.md),
                ; and also that TiDB uses repeatable read to mean SI
                ; (https://github.com/pingcap/docs/blob/master/sql/transaction-isolation.md).
                ; I've tried testing both with an explicitly provided
                ; repeatable read isolation level, and without an explicit
                ; level; both report the current transaction isolation level as
                ; 4 (repeatable read), and have identical effects.
                ;(j/with-db-transaction [~c ~conn :isolation :repeatable-read]
                (j/with-db-transaction [~c ~conn]
                  ; PingCAP added this start-transaction statement below. I
                  ; have concerns about this--it's not clear to me whether
                  ; starting, and not committing, this nested transaction does
                  ; the right thing. In particular, PingCAP has some docs
                  ; (https://github.com/pingcap/docs/blob/master/sql/transaction.md)
                  ; which say "If at this time, the current Session is in the
                  ; process of a transaction, a new transaction is started
                  ; after the current transaction is committed." which does NOT
                  ; seem like it's what we want, because at this point, we're
                  ; already inside a transaction!
                  ; (j/execute! ~c ["start transaction with consistent snapshot"])
                  ;(info :isolation (-> ~c
                  ;                     j/db-find-connection
                  ;                     .getTransactionIsolation))
                  ~@body)))))

(defmacro with-conn
  [[c node] & body]
  `(j/with-db-connection [~c (conn-spec ~node)]
     (when (.isClosed (j/db-find-connection ~c))
       (Thread/sleep 1000)
       (throw (ex-info "Connection not yet ready."
                       {:type :conn-not-ready})))
     ~@body))
