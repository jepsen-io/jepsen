(ns jepsen.nemesis.membership.state
  "This namespace defines the protocol for nemesis membership state
  machines---how to find the current view from a node, how to merge node views
  together, how to generate, apply, and complete operations, etc.

  States should be Clojure defrecords, and have several special keys:

    :node-views     A map of nodes to the view of the cluster state from that
                    particular node.

    :view           The merged view of the cluster state.

    :pending        A set of [op op'] pairs we've applied to the cluster,
                    but we're not sure if they're resolved yet.

  All three of these keys will be initialized and merged into your State for
  you by the membership nemesis."
  (:refer-clojure :exclude [resolve]))

(defprotocol State
  (setup! [this test]
          "Performs a one-time initialization of state. Should return a new
          state. This is a good place to open network connections or set up
          mutable resources.")

  (node-view [this test node]
             "Returns the view of the cluster from a particular node. Return
             `nil` to indicate the view is currently unknown; the membership
             system will ignore nil results.")

  (merge-views [this test]
               "Derive a new :view from this state's current :node-views.
               Used as our authoritative view of the cluster.")

  (fs [this]
      "A set of all possible op :f's we might generate.")

  (op [this test]
      "Returns an operation we could perform next--or :pending if no
      operation is available.")

  (invoke! [this test op]
           "Applies an operation we generated. Returns a completed op, or a
           tuple of [op, state'].")


  (resolve [this test]
           "Called repeatedly on a state to evolve it towards some fixed new
           state. A more general form of resolve-op.")

  (resolve-op [this test [op op']]
             "Called with a particular pair of operations (both invocation and
             completion). If that operation has been resolved, returns a new
             version of the state. Otherwise, returns nil.")

  (teardown! [this test]
             "Called at the end of the test to dispose of this State. This is
             your opportunity to close network connections etc."))
