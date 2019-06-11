(ns tidb.long-fork
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [generator :as gen]]
            [jepsen.tests.long-fork :as lf]
            [tidb [sql :as c :refer :all]
                  [txn :as txn]]))

(defn workload
  [opts]
  (assoc (lf/workload 10)
         :client (txn/client {:val-type "int"})))
