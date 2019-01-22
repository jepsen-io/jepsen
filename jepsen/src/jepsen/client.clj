(ns jepsen.client
  "Applies operations to a database."
  (:require [clojure.tools.logging :refer :all]
            [clojure.reflect :refer [reflect]]
            [jepsen.util :as util]
            [dom-top.core :refer [with-retry]]))

(defprotocol Client
  ; TODO: this should be open, not open!
  (open! [client test node]
          "Set up the client to work with a particular node. Returns a client
          which is ready to accept operations via invoke! Open *should not*
          affect the logical state of the test; it should not, for instance,
          modify tables or insert records.")
  (close! [client test]
          "Close the client connection when work is completed or an invocation
           crashes the client. Close should not affect the logical state of the
          test.")
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
    (close!    [this test])))

(defn open-compat!
  "Attempts to call `open!` on the given client. If `open!` does not
  exist, we assume a legacy implementation of `setup!`."
  [client test node]
  (try
    (let [client (open! client test node)
          _      (setup! client test)]
      (assert client (str "Expected a client, but `open!` returned " (pr-str client) " instead."))
      client)
    (catch java.lang.AbstractMethodError e
      (warn "DEPRECATED: `jepsen.client/open!` not implemented. Falling back to deprecated semantics of `jepsen.client/setup!`. You should separate your client's `setup!` function into `open!` and `setup!`. See the jepsen.client documentation for details.")
      (let [client (setup! client test node)]
        (assert client (str "Expected a client, but `setup!` returned " (pr-str client) " instead."))
        client))))

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
  [client test]
  (if (closable? client)
    (do (teardown! client test)
        (close! client test))
    (do (warn "DEPRECATED: `jepsen.client/close!` not implemented. Falling back to deprecated semantics of `jepsen.client/teardown!`. You should separate your client's `teardown!` function into `close!` and `teardown!`. See the jepsen.client documentation for details.")
        (teardown! client test))))
