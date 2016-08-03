(ns jepsen.cockroach.error
  "Early attempts at error handling resulted in a tangled, ad-hoc mess. Here,
  we're going to do it right. Clean. Understandable. Aesthetically pleasing. In
  this namespace, read every `let` with a deep, full breath. Feel the
  life-giving air within your lungs. That's better, isn't it? Refreshing.
  That's the feeling this namespace should give you.

  For reasons having to do with jdbc pooling and thread re-use, the details of
  which are lost to the mists of time, we're going to handle our own connection
  pooling. So we're going to wrap connections in an atom--when operations on a
  conn fail, we'll close the conn and open a new one."
  (:require [clojure.java.jdbc :as j]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [jepsen.util :as util]))

(defn client
  "A client is a stateful construct for talking to a database. It includes a
  function which can generate connections (:conn-fn) and a current connection
  atom (:conn)."
  [open close]
  {:open  open
   :conn  (atom nil)
   :close close})

(defn conn
  "Active connection for a client, if one exists."
  [client]
  @(:conn client))

(defn open!
  "Given a client, opens a connection. Noop if conn is already open."
  [client]
  (locking client
    (when-not (conn client)
      (reset! (:conn client) ((:open client)))))
  client)

(defn close!
  "Closes a client."
  [client]
  (locking client
    (when-let [c (conn client)]
      ((:close client) c)
      (reset! (:conn client) nil)))
  client)

(defn reopen!
  "Reopens a client's connection."
  [client]
  (locking client
    (-> client close! open!)))

(defmacro with-conn
  "Takes a connection from the client, and evaluates body with that connection
  bound to c. If any Exception is thrown, closes the connection and opens a new
  one. Locks client."
  [[c client] & body]
  `(locking ~client
     (try (let [~c (conn ~client)]
            ~@body)
          (catch Exception e#
            (info "Encountered error with DB connection open; reopening.")
            (reopen! ~client)
            (throw e#)))))

(defmacro with-timeout
  "Like util/timeout, but throws (RuntimeException. \"timeout\") for timeouts.
  Throwing means that when we time out inside a with-conn, the connection state
  gets reset, so we don't accidentally hand off the connection to a later
  invocation with some incomplete transaction."
  [dt & body]
  `(util/timeout ~dt (throw (RuntimeException. "timeout"))
                 ~@body))

(defmacro with-txn-retry
  "Catches PSQL 'restart transaction' errors and retries body a bunch of times,
  with exponential backoffs."
  [& body]
  `(util/with-retry [attempts# 30
                     backoff# 20]
     ~@body
     (catch org.postgresql.util.PSQLException e#
       (if (and (pos? attempts#)
                (re-find #"ERROR: restart transaction: retry txn"
                         (.getMessage e#)))
         (do (Thread/sleep backoff#)
             (~'retry (dec attempts#)
                      (* backoff# (+ 4 (* 0.5 (- (rand) 0.5))))))
         (throw e#)))))

(defn exception->op
  "Takes an exception and maps it to a partial op, like {:type :info, :error
  ...}. nil if unrecognized."
  [e]
  (let [m (.getMessage e)]
    (condp instance? e
      java.sql.SQLTransactionRollbackException
      {:type :fail, :error [:rollback m]}

      java.sql.BatchUpdateException
      (let [m' (if (re-find #"getNextExc" m)
                 (.getMessage (.getNextException e))
                 m)]
        {:type :info, :error [:batch-update m']})

      org.postgresql.util.PSQLException
      {:type :info, :error [:psql-exception m]}

      (condp re-find m
        #"^timeout$"
        {:type :info, :error :timeout}))))

(defmacro with-exception->op
  "Takes an operation and a body. Evaluates body, catches exceptions, and maps
  them to ops with :type :info and a descriptive :error."
  [op & body]
  `(try ~@body
        (catch Exception e#
          (if-let [ex-op# (exception->op e#)]
            (merge ~op ex-op#)
            (throw e#)))))
