(ns yugabyte.long-fork
  "Looks for instances of long fork: a snapshot isolation violation involving
  incompatible orders of writes to disparate objects"
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [jepsen.tests.long-fork :as lf]))

(defn workload
  [opts]
  (lf/workload 3))
