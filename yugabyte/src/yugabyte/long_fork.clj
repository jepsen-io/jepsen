(ns yugabyte.long-fork
  "Looks for instances of long fork: a snapshot isolation violation involving
  incompatible orders of writes to disparate objects"
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [jepsen.tests.long-fork :as lf]
            [yugabyte.generator :as ygen]))

(defn workload
  [opts]
  (ygen/workload-with-op-index (lf/workload 3)))
