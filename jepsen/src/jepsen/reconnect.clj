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
            [jepsen.util :as util]))

(defn wrapper
  "A wrapper is a stateful construct for talking to a database. Options:

 :name     An optional name for this wrapper (for debugging logs)
  :open     A function which generates a new conn
  :close    A function which closes a conn
  :log?     Whether to log reconnect messages
  :retries  Number of times to transparently retry operations after reconnect"
  [options]
  (assert (ifn? (:open options)))
  (assert (ifn? (:close options)))
  {:open    (:open options)
   :close   (:close options)
   :retries (:retries options 0)
   :log?    (:log? options)
   :conn    (atom nil)})

(defn conn
  "Active connection for a wrapper, if one exists."
  [wrapper]
  @(:conn wrapper))

(defn open!
  "Given a wrapper, opens a connection. Noop if conn is already open."
  [wrapper]
  (locking wrapper
    (when-not (conn wrapper)
      (reset! (:conn wrapper) ((:open wrapper)))))
  wrapper)

(defn close!
  "Closes a wrapper."
  [wrapper]
  (locking wrapper
    (when-let [c (conn wrapper)]
      ((:close wrapper) c)
      (reset! (:conn wrapper) nil)))
  wrapper)

(defn reopen!
  "Reopens a wrapper's connection."
  [wrapper]
  (locking wrapper
    (-> wrapper close! open!)))

(defmacro with-conn
  "Takes a connection from the wrapper, and evaluates body with that connection
  bound to c. If any Exception is thrown, closes the connection and opens a new
  one. Locks wrapper."
  [[c wrapper] & body]
  `(util/with-retry [retries# (:retries ~wrapper)]
     (try (let [~c (conn ~wrapper)]
          ~@body)
        (catch Exception e#
          (when (:log? ~wrapper)
            (warn (str "Encountered error with conn "
                       (pr-str (:name wrapper))
                      "; reopening")))
          (reopen! ~wrapper)
          (if (pos? retries#)
            (~'retry (dec retries#))
            (throw e#))))))
