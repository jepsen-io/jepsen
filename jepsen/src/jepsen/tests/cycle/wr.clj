(ns jepsen.tests.cycle.wr
  "A test which looks for cycles in write/read transactions. Writes are assumed
  to be unique, but this is the only constraint. See elle.rw-register for docs."
  (:require [elle [rw-register :as r]]
            [jepsen [checker :as checker]
                    [generator :as gen]
                    [store :as store]]))

(defn gen
  "Wrapper around rw-checker/gen."
  [opts]
  (r/gen opts))

(defn checker
  "Full checker for write-read registers. Options are:

    :additional-graphs      A collection of graph analyzers (e.g. realtime)
                            which should be merged with our own dependency
                            graph.
    :anomalies              A collection of anomalies which should be reported,
                            if found.
    :sequential-keys?       Assume that each key is independently sequentially
                            consistent, and use each processes' transaction
                            order to derive a version order.
    :linearizable-keys?     Assume that each key is independently linearizable,
                            and use the realtime process order to derive a
                            version order.
    :wfr-keys?              Assume that within each transaction, writes follow
                            reads, and use that to infer a version order.

  Supported anomalies are:

    :G0   Write Cycle. A cycle comprised purely of write-write deps.
    :G1a  Aborted Read. A transaction observes data from a failed txn.
    :G1b  Intermediate Read. A transaction observes a value from the middle of
          another transaction.
    :G1c  Circular information flow. A cycle comprised of write-write and
          write-read edges.
    :G-single  An dependency cycle with exactly one anti-dependency edge.
    :G2   A dependency cycle with at least one anti-dependency edge.
    :internal Internal consistency anomalies. A transaction fails to observe
              state consistent with its own prior reads or writes.

  :G2 implies :G-single and :G1c. :G1 implies :G1a, :G1b, and :G1c. G1c implies
  G0. The default is [:G2 :G1a :G1b :internal], which catches everything."
  ([]
   (checker {}))
  ([opts]
   (reify checker/Checker
     (check [this test history checker-opts]
       (r/check (assoc opts :directory
                       (.getCanonicalPath
                         (store/path! test (:subdirectory opts) "elle")))
                history)))))
