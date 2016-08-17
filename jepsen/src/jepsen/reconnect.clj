(ns jepsen.reconnect
  "Stateful wrappers for automatically reconnecting network clients.

  A wrapper is a map with a connection atom `conn` and a pair of functions:
  `(open)`, which opens a new connection, and `(close conn)`, which closes a
  connection. We use these to provide a with-conn macro that acquires the
  current connection from a wrapper, evaluates body, and automatically
  closes/reopens the connection when errors occur.

  Connect/close/reconnect lock the wrapper, but multiple threads may acquire
  the current connection at once."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.util :as util])
  (:import (java.util.concurrent.locks ReentrantReadWriteLock)))

(defn wrapper
  "A wrapper is a stateful construct for talking to a database. Options:

  :name     An optional name for this wrapper (for debugging logs)
  :open     A function which generates a new conn
  :close    A function which closes a conn
  :log?     Whether to log reconnect messages"
  [options]
  (assert (ifn? (:open options)))
  (assert (ifn? (:close options)))
  {:open    (:open options)
   :close   (:close options)
   :log?    (:log? options)
   :name    (:name options)
   :lock    (ReentrantReadWriteLock.)
   :conn    (atom nil)})

(defmacro with-lock
  [wrapper lock-method & body]
  `(let [lock# (~lock-method ^ReentrantReadWriteLock (:lock ~wrapper))]
     (.lock lock#)
     (try ~@body
          (finally
            (.unlock lock#)))))

(defmacro with-read-lock
  [wrapper & body]
  `(with-lock ~wrapper .readLock ~@body))

(defmacro with-write-lock
  [wrapper & body]
  `(with-lock ~wrapper .writeLock ~@body))

(defn conn
  "Active connection for a wrapper, if one exists."
  [wrapper]
  @(:conn wrapper))

(defn open!
  "Given a wrapper, opens a connection. Noop if conn is already open."
  [wrapper]
  (with-write-lock wrapper
    (when-not (conn wrapper)
      (reset! (:conn wrapper) ((:open wrapper)))))
  wrapper)

(defn close!
  "Closes a wrapper."
  [wrapper]
  (with-write-lock wrapper
    (when-let [c (conn wrapper)]
      ((:close wrapper) c)
      (reset! (:conn wrapper) nil)))
  wrapper)

(defn reopen!
  "Reopens a wrapper's connection."
  [wrapper]
  (with-write-lock wrapper
    (-> wrapper close! open!)))

(defmacro with-conn
  "Acquires a read lock, takes a connection from the wrapper, and evaluates
  body with that connection bound to c. If any Exception is thrown, closes the
  connection and opens a new one."
  [[c wrapper] & body]
  `(try (with-read-lock ~wrapper
          (let [~c (conn ~wrapper)]
            (assert ~c)
            ~@body))
        (catch Exception e#
          (when (:log? ~wrapper)
            (warn (str "Encountered error with conn "
                       (pr-str (:name wrapper))
                       "; reopening")))
          (reopen! ~wrapper)
          (throw e#))))
