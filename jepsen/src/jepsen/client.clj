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
    (teardown! client test)))

(defn validate
  "Wraps a client, validating that its return types are what you'd expect."
  [client]
  (Validate. client))

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
