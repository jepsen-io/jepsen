(ns aerospike.nemesis
  (:require [aerospike [support :as s]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [util :refer [meh random-nonempty-subset]]]))

; Nemeses

(defn capped-conj
  "Conj's x into set s so long as (count x) would remain at cap or lower."
  [s x cap]
  (let [s' (conj s x)]
    (if (< cap (count s')) s s')))

(defn kill-nemesis
  "Takes a maximum number of dead processes to allow. Also takes an atom to
  track which nodes are dead. Kills processes with :f :kill, restarts them with
  :f :restart. :value op is a set of nodes to affect."
  [max-dead dead]
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc
        op :value
        (case (:f op)
          :kill  (c/on-nodes test (:value op)
                             (fn [test node]
                               (if ((swap! dead capped-conj node max-dead)
                                    node)
                                 (do (meh (c/su (c/exec :killall :-9 :asd)))
                                     :killed)
                                 :still-alive)))
          :restart (c/on-nodes test (:value op)
                             (fn [test node]
                               (c/su (c/exec :service :aerospike :restart))
                               (swap! dead disj node)
                               :started)))))

    (teardown! [this test])))

(defn kill-gen
  "Randomized kill operations."
  [test process]
  {:type :info, :f :kill, :value (random-nonempty-subset (:nodes test))})

(defn restart-gen
  "Randomized restart operations."
  [test process]
  {:type :info, :f :restart, :value (random-nonempty-subset (:nodes test))})

(defn kill-restart-gen
  "Mix of kill and restart operations."
  []
  (gen/mix [kill-gen restart-gen]))

(defn killer
  "A combined nemesis and generator for killing nodes. Options:

  :max-dead-nodes   number of nodes allowed to be down simultaneously"
  [opts]
  (let [dead (atom #{})]
    {:nemesis (kill-nemesis (:max-dead-nodes opts) dead)
     :generator (kill-restart-gen)
     :final-generator (gen/once
                        (fn [test _]
                          {:type :info, :f :restart, :value (:nodes test)}))}))
