(ns jepsen.client
  "Applies operations to a database."
  (:require [clojure.tools.logging :refer :all]
            [clojure.reflect :refer [reflect]]))

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

;; FIXME Docstring
(defn reopen! [client test node]
  (try (close! client test)
       (catch RuntimeException _))
  (open! client test node))

(defn open-compat
  "Attempts to call `open!`, if `open!` does not exist, assumes legacy implementation of `setup!`."
  [client test node]
  (try
    ;; Nemeses are treated the same as clients for nodes that have already been set up.
    (let [lock (get (:setup-locks test) node)]
      (if (and lock (compare-and-set! lock false true))
        (do (open! client test node)
            (setup! client test))
        (open! client test node)))

    ;; TODO This has sometimes been IllegalArgumentException, maybe this is related to local compilation? TEST IT
    (catch java.lang.AbstractMethodError e
      (warn "DEPRECATED: `jepsen.client/open!` not implemented. Falling back to deprecated semantics of `jepsen.client/setup!`.")
      (setup! client test node))))

(defn closable? [client]
  (->> client reflect :members (map :name) (some #{'close_BANG_})))

(defn close-compat
  "Inspects the client for `close!` method. If `close!` not implemented, we assume a
  legacy `teardown!` implementation that cleans the database and closes the client."
  [client test]
  (if (closable? client)
    (do (teardown! client test)
        (close! client test))
    (do (warn "DEPRECATED: `jepsen.client/close!` not implemented. Falling back to deprecated semantics of `jepsen.client/teardown!`.")
        (teardown! client test))))
