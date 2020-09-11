(ns jepsen.nemesis.membership.state
  "This namespace defines the protocol for nemesis membership state
  machines---how to find the current view from a node, how to merge node views
  together, how to generate, apply, and complete operations, etc.")

(defprotocol State
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
           "Applies an operation we generated. Returns a completed op.")

  (resolve [this test]
           "Called repeatedly on a state to evolve it towards some fixed new
           state. A more general form of resolve-op.")

  (resolve-op [this test [op op']]
             "Is the given operation (given as both its invocation and
             completion) complete?"))
