(ns jepsen.client
  "Applies operations to a database."
  (:require [clojure.tools.logging :refer :all]
            [clojure.reflect :refer [reflect]]
            [jepsen.util :as util]
            [dom-top.core :refer [with-retry]]
            [slingshot.slingshot :refer [try+ throw+]]))

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
  (setup! [client test]
          "Called to set up database state for testing.")
  (invoke! [client test operation]
           "Apply an operation to the client, returning an operation to be
           appended to the history. For multi-stage operations, the client may
           reach into the test and conj onto the history atom directly.")
  (teardown! [client test]
           "Tear down database state when work is complete."))

(defprotocol Reusable
  (reusable? [client test]
             "If true, this client can be re-used with a fresh process after a
             call to `invoke` throws or returns an `info` operation. If false
             (or if this protocol is not implemented), crashed clients will be
             closed and new ones opened to replace them."))

(defn is-reusable?
  "Wrapper around reusable?; returns false when not implemented."
  [client test]
  ; satisfies? Reusable is somehow true for records which DEFINITELY don't
  ; implement it and I don't know how this is possible, so we're falling back
  ; to IllegalArgException
  (try (reusable? client test)
       (catch IllegalArgumentException e
         false)))

(def noop
  "Does nothing."
  (reify Client
    (setup!    [this test])
    (teardown! [this test])
    (invoke!   [this test op] (assoc op :type :ok))
    (open!     [this test node] this)
    (close!    [this test])))

(defn closable?
  "Returns true if the given client implements method `close!`."
  [client]
  (->> client
       reflect
       :members
       (map :name)
       (some #{'close_BANG_})))

(defrecord Validate [client]
  Client
  (open! [this test node]
    (let [res (open! client test node)]
      (when-not (satisfies? Client res)
        (throw+ {:type    ::open-returned-non-client
                 :got     res}
                nil
                "expected open! to return a Client, but got %s instead"
                (pr-str res)))
      (Validate. res)))

  (close! [this test]
    (close! client test))

  (setup! [this test]
          (Validate. (setup! client test)))

  (invoke! [this test op]
    (let [op' (invoke! client test op)]
      (let [problems
            (cond-> []
              (not (map? op'))
              (conj "should be a map")

              (not (#{:ok :info :fail} (:type op')))
              (conj ":type should be :ok, :info, or :fail")

              (not= (:process op) (:process op'))
              (conj ":process should be the same")

              (not= (:f op) (:f op'))
              (conj ":f should be the same"))]
        (when (seq problems)
          (throw+ {:type      ::invalid-completion
                   :op        op
                   :op'       op'
                   :problems  problems})))
        op'))

  (teardown! [this test]
    (teardown! client test))

  Reusable
  (reusable? [this test]
    (reusable? client test)))

(defn validate
  "Wraps a client, validating that its return types are what you'd expect."
  [client]
  (Validate. client))

(defrecord Timeout [timeout-fn client]
  Client
  (open! [this test node]
    (Timeout. timeout-fn (open! client test node)))

  (setup! [this test]
    (Timeout. timeout-fn (setup! client test)))

  (invoke! [this test op]
    (let [ms (timeout-fn op)]
      (util/timeout ms (assoc op :type :info, :error ::timeout)
                    (invoke! client test op))))

  (teardown! [this test]
    (teardown! client test))

  (close! [this test]
    (close! client test))

  Reusable
  (reusable? [this test]
    (reusable? client test)))

(defn timeout
  "Sometimes a client library's own timeouts don't work reliably. This takes
  either a timeout as a number of ms, or a function (f op) => timeout-in-ms,
  and a client. Wraps that client in a new one which automatically times out
  operations that take longer than the given timeout. Timed out operations have
  :error :jepsen.client/timeout."
  [timeout-or-fn client]
  (if (number? timeout-or-fn)
    (Timeout. (constantly timeout-or-fn) client)
    (Timeout. timeout-or-fn client)))

(defmacro with-client
  "Analogous to with-open. Takes a binding of the form [client-sym
  client-expr], and a body. Binds client-sym to client-expr (presumably,
  client-expr opens a new client), evaluates body with client-sym bound, and
  ensures client is closed before returning."
  [[client-sym client-expr] & body]
  `(let [~client-sym ~client-expr]
     (try
       ~@body
       (finally
         (close! ~client-sym test)))))

