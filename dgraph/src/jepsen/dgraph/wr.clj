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
   :checker   (checker/compose
                {:wr (wr/checker {:wfr-keys?           true
                                  :sequential-keys?    true
                                  :anomalies           [:G0 :G1c :G-single :G1a
                                                        :G1b :internal]
                                  :additional-graphs   [cycle/realtime-graph]})
                 :timeline (timeline/html)})
   :generator (->> (append/wr-txns {:key-count  4
                                    :min-length 2
                                    :max-length 4
                                    :max-writes-per-key 16})
                   (map (fn [txn] {:type :invoke, :f :txn, :value txn}))
                   gen/seq)})
