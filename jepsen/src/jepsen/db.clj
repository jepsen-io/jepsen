(ns jepsen.db
  "Allows Jepsen to set up and tear down databases.")

(defprotocol DB
  (setup!     [db test node] "Set up the database on this particular node.")
  (teardown!  [db test node] "Tear down the database on this particular node."))

(defprotocol Primary
  (setup-primary! [db test node] "Performs one-time setup on a single node."))

(defprotocol LogFiles
  (log-files [db test node] "Returns a sequence of log files for this node."))

(def noop
  "Does nothing."
  (reify DB
    (setup!    [db test node])
    (teardown! [db test node])))

(defn cycle!
  "Tries to tear down, then set up, the given DB."
  [db test node]
  (try (teardown! db test node)
       (catch RuntimeException _))
  (setup! db test node))
