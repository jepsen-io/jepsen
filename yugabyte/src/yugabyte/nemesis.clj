(ns yugabyte.nemesis
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
             [nemesis :as nemesis]
             [util :as util :refer [meh timeout]]]
            [slingshot.slingshot :refer [try+]]
            [yugabyte.common :refer :all]
))

(def nemesis-delay 5) ; Delay between nemesis cycles.
(def nemesis-duration 5) ; Duration of single nemesis cycle.

(defn kill!
  [node process opts]
  (meh (c/exec :pkill opts process))
  (info (c/exec :echo :pkill opts process))
  (c/exec (c/lit (str "! ps -ce | grep" process)))
  (info node process "killed.")
  :killed)

(defn none
  "No-op nemesis"
  []
  nemesis/noop
)

(defn tserver-killer
  "Kills a random node tserver on start, restarts it on stop."
  [& kill-opts]
  (nemesis/node-start-stopper
    rand-nth
    (fn start [test node] (kill! node "yb-tserver" kill-opts))
    (fn stop  [test node] (start-tserver! node))))

(defn master-killer
  "Kills a random node master on start, restarts it on stop."
  [& kill-opts]
  (nemesis/node-start-stopper
    (comp rand-nth running-masters)
    (fn start [test node] (kill! node "yb-master" kill-opts))
    (fn stop  [test node] (start-master! node))))

(defn node-killer
  "Kills a random node tserver and master on start, restarts it on stop."
  [& kill-opts]
  (nemesis/node-start-stopper
    rand-nth
    (fn start [test node]
      (kill! node "yb-tserver" kill-opts)
      (kill! node "yb-master" kill-opts)
    )
    (fn stop  [test node]
      (start-master! node)
      (start-tserver! node)
    )
  )
)

(def nemeses
  "Supported nemeses"
  {"none"                       `(none)
   "start-stop-tserver"         `(tserver-killer)
   "start-kill-tserver"         `(tserver-killer :-9)
   "start-stop-master"          `(master-killer)
   "start-kill-master"          `(master-killer :-9)
   "start-stop-node"            `(node-killer)
   "start-kill-node"            `(node-killer :-9)
   "partition-random-halves"    `(nemesis/partition-random-halves)
   "partition-random-node"      `(nemesis/partition-random-node)
   "partition-majorities-ring"  `(nemesis/partition-majorities-ring)
  }
)

(defn get-nemesis-by-name
  [name]
  (eval (get nemeses name))
)