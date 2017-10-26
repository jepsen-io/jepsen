(ns jepsen.client
  "Applies operations to a database."
  (:require [clojure.tools.logging :refer [warn]]))

(defprotocol Client
  (open! [client test node]
          "Set up the client to work with a particular node. Returns a client
          which is ready to accept operations via invoke!")
  (close! [client test]
          "Close the client connection when work is completed or an invocation
           crashes the client.")
  (setup! [client test] [client test node]
          "Called once to set up database state for testing. 3 arity form is
           deprecated and will be removed in a future jepsen version.")
  (invoke! [client test operation]
           "Apply an operation to the client, returning an operation to be
           appended to the history. For multi-stage operations, the client may
           reach into the test and conj onto the history atom directly.")
  (teardown! [client test]
           "Tear down the client when work is complete."))

(def noop
  "Does nothing."
  (reify Client
    (setup!    [this test])
    (teardown! [this test])
    (invoke!   [this test op] (assoc op :type :ok))
    (open!     [this test client] this)
    (close!    [this test])))

(defn reopen!
  "Tries to close then open the given client"
  [client test node]
  (try (close! client test)
       (catch RuntimeException _))
  (open! client test node))

(defn open-compat
  "Attempts to call `open!`, if `open!` does not exist, assumes legacy implementation of `setup!`."
  [client test node]
  (try
    (open! client test node)
    ;; TODO This has sometimes been IllegalArgumentException, maybe this is related to local compilation? TEST IT
    (catch java.lang.AbstractMethodError e
      (warn "DEPRECATED: open! not implemented on client, falling back to legacy setup!")
      (setup! client test node))))

(defn close-compat
  "Inspects the client for `close!` method. If `close!` not implemented, we assume a
  legacy `teardown!` implementation that cleans the database and closes the client."
  [client test]
  (if (->> client .getClass .getMethods (contains? 'close_BANG_))
    (do
      (teardown! client test)
      (close! client test)
      (warn "CALLED the first method in close-compat, which shouldn't even be called cause close doesn't exist")
      )
    (do
      (warn "DEPRECATED: close! not implemented on client, falling back to legacy teardown!")
      (teardown! client test))))

(comment
  make sure we set up at most once
  (def atom setup? false)
  (defn perform-setup []
    (try
      (compare-and-set! setup? false true)
      (catch Exception e
        (compare-and-set! setup false false))))

  inside of each worker
  make sure every client gets a connection

  make sure that an exception within a worker

  make sure every client closes its connection by the time we exit

  make sure tear down once)
