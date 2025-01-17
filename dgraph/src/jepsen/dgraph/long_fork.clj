(ns jepsen.dgraph.long-fork
  (:require [jepsen.tests.long-fork :as lf]
            [jepsen.dgraph.client :as c]))

(defn workload
  [opts]
  (assoc (lf/workload 3)
         :client (c/txn-client {:blind-insert-on-write? true})))
