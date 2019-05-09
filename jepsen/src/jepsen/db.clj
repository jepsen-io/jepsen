(ns jepsen.db
  "Allows Jepsen to set up and tear down databases."
  (:require [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen [control :as control]
                    [util :refer [fcatch]]]))

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

(def cycle-tries
  "How many tries do we get to set up a database?"
  3)

(defn cycle!
  "Takes a test, and tears down, then sets up, the database on all nodes
  concurrently.

  If any call to setup! or setup-primary! throws :type ::setup-failed, we tear
  down and retry the whole process up to `cycle-tries` times."
  [test]
  (let [db (:db test)]
    (loop [tries cycle-tries]
      ; Tear down every node
      (info "Tearing down DB")
      (control/on-nodes test (partial teardown! db))

      ; Start up every node
      (if (= :retry (try+
                      ; Normal set up
                      (info "Setting up DB")
                      (control/on-nodes test (partial setup! db))

                      ; Set up primary
                      (when (satisfies? Primary db)
                        ; TODO: refactor primary out of core and here and
                        ; into util.
                        (info "Setting up primary" (first (:nodes test)))
                        (control/on-nodes test [(first (:nodes test))]
                                          (partial setup-primary! db)))

                      nil
                      (catch RuntimeException e
                        (info :caught e))
                      (catch [:type ::setup-failed] e
                        (if (< 1 tries)
                          (do (info :throwable (pr-str (type (:throwable &throw-context))))
                              (warn (:throwable &throw-context)
                                    "Unable to set up database; retrying...")
                              :retry)

                          ; Out of tries, abort!
                          (throw+ e)))))
        (recur (dec tries))))))
