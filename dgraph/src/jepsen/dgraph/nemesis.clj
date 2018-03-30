(ns jepsen.dgraph.nemesis
  "Failure modes!"
  (:require [jepsen [generator :as gen]
                    [util :as util]
                    [nemesis :as nemesis]]
            [jepsen.dgraph [support :as s]]))

(defn alpha-killer
  "Responds to :start by killing alpha on random nodes, and to :stop by
  resuming them."
  []
  (nemesis/node-start-stopper util/random-nonempty-subset
                              s/stop-alpha!
                              s/start-alpha!))

(defn zero-killer
  "Responds to :start by killing zero on random nodes, and to :start by
  resuming them."
  []
  (nemesis/node-start-stopper util/random-nonempty-subset
                              s/stop-zero!
                              s/start-zero!))

(defn full-nemesis
  "Can kill and restart all processes and initiate network partitions."
  [opts]
  (nemesis/compose
    {{:kill-alpha       :start
      :restart-alpha    :stop}  (alpha-killer)
     {:kill-zero        :start
      :restart-zero     :stop}  (zero-killer)
     {:start-partition  :start
      :stop-partition   :stop}  (nemesis/partition-random-halves)}))

(defn op
  "Construct a nemesis op"
  [f]
  {:type :info, :f f, :value nil})

(defn composite-generator
  "Takes options:"
  [opts]
  (->> (gen/mix [(gen/seq (cycle (map op [:kill-alpha :restart-alpha])))
                 (gen/seq (cycle (map op [:kill-zero  :restart-zero])))])
  ;           (gen/seq (cycle (map op [:start-partition :stop-partition])))
       (gen/stagger 15)))

(defn nemesis
  "Composite nemesis and generator"
  [opts]
  {:nemesis   (full-nemesis opts)
   :generator (composite-generator opts)
   :final-generator (gen/seq (map op [;:stop-partition
                                      :restart-alpha
                                      :restart-zero]))})
