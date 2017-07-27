(ns tidb.client
  (:require [clojure.string :as str]
            [jepsen
              [util :refer [timeout]]
            ]
            [clojure.java.jdbc :as j]
  )
)

(defn conn-spec
  "jdbc connection spec for a node."
  [node]
  {:classname   "org.mariadb.jdbc.Driver"
   :subprotocol "mariadb"
   :subname     (str "//" (name node) ":4000/test")
   :user        "root"
   :password    ""
  }
)

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
        ))

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
  [op [c node] & body]
  `(timeout 5000 (assoc ~op :type :info, :value :timed-out)
           (with-error-handling ~op
             (with-txn-retries
               (j/with-db-transaction [~c (conn-spec ~node) :isolation :serializable]
                 (j/execute! ~c ["start transaction with consistent snapshot"])
                 ~@body)))))
