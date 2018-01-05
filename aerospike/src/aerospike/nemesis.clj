(ns aerospike.nemesis
  (:require [aerospike [support :as s]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [util :refer [meh random-nonempty-subset]]]
            [jepsen.nemesis.time :as nt]))

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
  [signal max-dead dead]
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc
        op :value
        (c/on-nodes
          test (:value op)
          (fn [test node]
            (case (:f op)
              :kill (if ((swap! dead capped-conj node max-dead) node)
                      (do (meh (c/su (c/exec :killall
                                             (str "-" signal)
                                             :asd)))
                          :killed)
                      :still-alive)

              :restart (do (c/su
                             (c/exec :service :aerospike :restart))
                           (swap! dead disj node)
                           :started)

              :revive
              (try (s/revive!)
                   (catch java.lang.RuntimeException e
                     (if (re-find #"Could not connect to node" (.getMessage e))
                       :not-running
                       (throw e))))

              :recluster
              (try (s/recluster!)
                   (catch java.lang.RuntimeException e
                     (if (re-find #"Could not connect to node" (.getMessage e))
                       :not-running
                       (throw e)))))))))

    (teardown! [this test])))

(defn kill-gen
  "Randomized kill operations."
  [test process]
  {:type :info, :f :kill, :value (random-nonempty-subset (:nodes test))})

(defn restart-gen
  "Randomized restart operations."
  [test process]
  {:type :info, :f :restart, :value (random-nonempty-subset (:nodes test))})

(defn revive-gen
  "Revive all nodes."
  [test process]
  {:type :info, :f :revive, :value (:nodes test)})

(defn recluster-gen
  "Recluster all nodes."
  [test process]
  {:type :info, :f :recluster, :value (:nodes test)})

(defn killer-gen-seq
  "Sequence of kills, restarts, revivals, and reclusterings"
  [test]
  (let [patterns (->> [[kill-gen]
                       [restart-gen]
                       ; Revive then recluster
                       (when-not (:no-revives test)
                         [revive-gen recluster-gen])]
                      (remove nil?)
                      vec)]
    (mapcat rand-nth (repeat patterns))))

(defn killer-gen
  "A mix of kills, restarts, revivals, and reclusterings"
  [test]
  (gen/seq (killer-gen-seq test)))

(defn full-nemesis
  "Handles kills, restarts, revives, reclusters, clock skew, and partitions."
  [opts]
  (nemesis/compose
    {{:partition-start :start
      :partition-stop  :stop} (nemesis/partition-random-halves)

     #{:kill :restart :revive :recluster} (kill-nemesis (if (:clean-kill opts)
                                                          15 ; SIGTERM
                                                          9) ; SIGKILL
                                                        (:max-dead-nodes opts)
                                                        (:dead opts))

     {:clock-reset  :reset
      :clock-bump   :bump
      :clock-strobe :strobe} (nt/clock-nemesis)}))

(defn full-gen
  [opts]
  "Generates kills, restarts, revives, reclusters, clock skews, and partitions."
  (->> [(when-not (:no-clocks opts) (gen/f-map {:strobe :clock-strobe
                                                :reset  :clock-reset
                                                :bump   :clock-bump}
                                               (nt/clock-gen)))
        (when-not (:no-kills opts) (killer-gen opts))
        (when-not (:no-partitions opts)
          (gen/seq (cycle [{:type :info, :f :partition-start}
                           {:type :info, :f :partition-stop}])))]
       (remove nil?)
       gen/mix))

(defn full
  "A combined nemesis and generator for all kinds of havoc. Options:

  :max-dead-nodes   number of nodes allowed to be down simultaneously"
  [opts]
  (let [dead (atom #{})
        opts (assoc opts :dead dead)]
    {:nemesis (full-nemesis opts)
     :generator (full-gen opts)
     :final-generator (gen/concat
                        (gen/once {:type :info, :f :partition-stop})
                        (gen/once {:type :info, :f :clock-reset})
                        (gen/once
                          (fn [test _]
                            {:type :info, :f :restart, :value (:nodes test)}))
                        (gen/sleep 10)
                        (gen/once revive-gen)
                        (gen/once recluster-gen))}))
