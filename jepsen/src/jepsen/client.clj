(ns jepsen.client
  "Applies operations to a database."
  (:require [clojure.tools.logging :refer :all]
            [clojure.reflect :refer [reflect]]
            [jepsen.util :as util]))

(defprotocol Client
  (open! [client test node]
          "Set up the client to work with a particular node. Returns a client
          which is ready to accept operations via invoke!")
  (close! [client test node]
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
    (open!     [this test node] this)
    (close!    [this test node])))

(defn reopen!
  "Takes the worker's current client, closes its connection and
  opens a new connection with the original."
  [client test node]
  (close! client test node)
  (open! client test node))

(defn open-compat!
  "Attempts to call `open!` on the given client. If `open!` does not
  exist, we assume a legacy implementation of `setup!`."
  [client test node]
  (try
    (let [client (open! client test node)
          _      (setup! client test)]
      client)
    (catch java.lang.AbstractMethodError e
      (warn "DEPRECATED: `jepsen.client/open!` not implemented. Falling back to deprecated semantics of `jepsen.client/setup!`.")
      (setup! client test node))))

(defn closable?
  "Returns true if the given client implements method `close!`."
  [client]
  (->> client
       reflect
       :members
       (map :name)
       (some #{'close_BANG_})))

(defn close-compat!
  "Inspects the client for `close!` method and calls `teardown!` then `close!`.
  If `close!` is not implemented, we assume a legacy implementation of `teardown!`."
  [client test node]
  (if (closable? client)
    (do (teardown! client test)
        (close! client test node))
    (do (warn "DEPRECATED: `jepsen.client/close!` not implemented. Falling back to deprecated semantics of `jepsen.client/teardown!`.")
        (teardown! client test))))
