(ns jepsen.ignite.nemesis
  "Apache Ignite nenesis"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [jepsen [ignite :as ignite]
                    [nemesis :as nemesis]]))

(def kill-node
  "Kills random node"
  (nemesis/node-start-stopper
    rand-nth
    (fn start [test node] (ignite/stop! node test))
    (fn stop [test node] (ignite/start! node test))))

(def partition-random-halves
  (nemesis/partition-random-halves))

