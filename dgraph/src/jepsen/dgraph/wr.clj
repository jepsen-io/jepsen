(ns jepsen.dgraph.wr
  "Performs random reads and writes of unique values. Constructs a dependency
  graph using realtime order (since dgraph is supposed to be linearizable) and
  via write-read edges, and looks for cycles in that graph."
  (:require [clojure.tools.logging :refer [info]]
            [clojure.core.reducers :as r]
            [fipp.edn :refer [pprint]]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]
                    [store :as store]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.wr :as wr]
            [jepsen.tests.cycle.append :as append]))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  {:client    (c/txn-client {})
   :checker   (wr/checker {:additional-graphs [cycle/realtime-graph]
                           :anomalies [:internal :G2]})
   :generator (->> (append/wr-txns {:key-count  5
                                    :min-length 1
                                    :max-length 3
                                    :max-writes-per-key 32})
                   (map (fn [txn] {:type :invoke, :f :txn, :value txn}))
                   gen/seq)})
