(ns jepsen.db
  "Allows Jepsen to set up and tear down databases.")

(defprotocol DB
  (setup!     [db test node] "Set up the database on this particular node.")
  (teardown!  [db test node] "Tear down the database on this particular node."))

(defprotocol Primary
  (setup-primary! [db test node] "Performs one-time setup on a single node."))

(def noop
  "Does nothing."
  (reify DB
    (setup!    [db test node])
    (teardown! [db test node])))
