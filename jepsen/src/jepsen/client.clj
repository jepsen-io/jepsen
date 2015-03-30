(ns jepsen.client
  "Applies operations to a database.")

(defprotocol Client
  (setup!  [client test node]
          "Set up the client to work with a particular node. Returns a client
          which is ready to accept operations via invoke!")
  (invoke! [client test operation]
           "Apply an operation to the client, returning an operation to be
           appended to the history. For multi-stage operations, the client may
           reach into the test and conj onto the history atom directly.")
  (teardown! [client test]
             "Tear down the client when work is complete."))

(def noop
  "Does nothing."
  (reify Client
    (setup!    [this test node] this)
    (teardown! [this test])
    (invoke!   [this test op] (assoc op :type :ok))))
