(ns jecci.common.long-fork
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
             [generator :as gen]]
            [jepsen.tests.long-fork :as lf]
            [jecci.common.txn :as txn]))

(defn workload
  [opts]
  (assoc (lf/workload 10)
    :client (txn/client {:val-type "int"})))
