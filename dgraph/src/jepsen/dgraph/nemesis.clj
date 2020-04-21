(ns jepsen.dgraph.nemesis
  "Failure modes!"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen [control :as c]
             [net :as net]
             [util :as util]
             [nemesis :as nemesis]]
            [jepsen.generator.pure :as gen]
            [jepsen.nemesis.time :as nt]
            [jepsen.control.util :as cu]
            [jepsen.dgraph [support :as s]]
            [jepsen.dgraph.trace :as t]
            [dom-top.core :refer [with-retry]]))

(defn alpha-killer
  "Responds to :start by killing alpha on random nodes, and to :stop by
  resuming them."
  []
  (nemesis/node-start-stopper identity ;util/random-nonempty-subset
                              s/stop-alpha!
                              s/start-alpha!))

(defn alpha-fixer
  "Alpha likes to fall over if zero isn't around on startup, so we'll issue
  speculative restarts."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (assoc op :value
             (c/on-nodes test (util/random-nonempty-subset (:nodes test))
                         (fn [test node]
                           (if (cu/daemon-running? s/alpha-pidfile)
                             :already-running
                             (do (s/start-alpha! test node)
                                 :restarted))))))

    (teardown! [this test])))

(defn zero-killer
  "Responds to :start by killing zero on random nodes, and to :start by
  resuming them."
  []
  (nemesis/node-start-stopper util/random-nonempty-subset
                              s/stop-zero!
                              s/start-zero!))

(defn tablet-mover
  "Moves tablets around at random"
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      (t/with-trace "nemesis.tablet-mover.setup!"
        this))

    (invoke! [this test op]
      (t/with-trace "nemesis.tablet-mover.invoke!"
        (let [state  (s/zero-state (rand-nth (:nodes test)))]
          (info :state (with-out-str (pprint state)))
          (if (= :timeout state)
            (assoc op :value :timeout)
            (let [groups (->> state :groups keys)
                  node   (s/zero-leader state)]
              (->> state
                   :groups
                   vals
                   (map :tablets)
                   (mapcat vals)
                   shuffle
                   (keep (fn [tablet]
                           (let [pred   (:predicate tablet)
                                 group  (:groupId tablet)
                                 group' (rand-nth groups)]
                             (when-not (= group group')
                               ;; Actually move tablet
                               (info "Moving" pred "from" group "to" group')
                               (try+
                                (s/move-tablet! node pred group')
                                (info "Moved" pred "from" group "to" group')
                                ;; Return predicate and new group
                                [pred [group group']]
                                (catch [:status 500] {:keys [body] :as e}
                                  (condp re-find body
                                    #"Unable to move reserved"
                                    (do
                                      (info "Unable to move reserved " pred "from" group "to" group')
                                      [pred [group group']])
                                    #"Server is not leader of this group"
                                    (do
                                      (info "Unable to move reserved " pred "from" group "to" group')
                                      [pred [group group']])
                                    (throw+ e))))))))
                   (into (sorted-map))
                   (assoc op :value)))))))

    (teardown! [this test])))

(defn bump-time
  "On randomly selected nodes, adjust the system clock by dt seconds.  Uses
  millisecond precision."
  [dt]
  (reify nemesis/Nemesis
    (setup! [this test]
      (with-retry [attempts 3]
        (nt/reset-time! test)
        (catch java.util.concurrent.ExecutionException e
          (if (< 0 attempts)
            (do
              (warn "Error resetting clock with NTP, retrying...")
              (Thread/sleep (rand-int 2000))
              (retry (dec attempts)))
            (throw+ e))))
      this)

    (invoke! [this test op]
      (assoc op :value
             (case (:f op)
               :start (c/with-test-nodes test
                        (if (< (rand) 0.5)
                          (do (nt/bump-time! dt)
                              dt)
                          0))
               :stop (c/with-test-nodes test
                       (nt/reset-time!)))))

    (teardown! [this test]
      (nt/reset-time! test))))

(defn skew
  [{:keys [skew] :as opts}]
  (case skew
    :huge  (bump-time 7500)
    :big   (bump-time 2000)
    :small (bump-time 250)
    :tiny  (bump-time 100)
    (bump-time 0)))

(defn full-nemesis
  "Can kill and restart all processes and initiate network partitions."
  [opts]
  (nemesis/compose
    {{:fix-alpha        :fix}   (alpha-fixer)
     {:kill-alpha       :start
      :restart-alpha    :stop}  (alpha-killer)
     {:kill-zero        :start
      :restart-zero     :stop}  (zero-killer)
     #{:move-tablet}            (tablet-mover)
     {:start-partition-halves  :start
      :stop-partition-halves   :stop} (nemesis/partition-random-halves)
     {:start-partition-ring    :start
      :stop-partition-ring     :stop} (nemesis/partition-majorities-ring)
     {:start-skew :start
      :stop-skew  :stop} (skew opts)}))

(defn op
  "Construct a nemesis op"
  [f]
  {:type :info, :f f})

(defn full-generator
  "Takes a nemesis specification map from the command line, and constructs a
  generator for the given types of nemesis operations, e.g. process kills and
  partitions."
  [opts]
  (->> [(when (:kill-alpha? opts)
          [(cycle [(op :kill-alpha)
                   (op :restart-alpha)])])
        (when (:kill-zero? opts)
          [(cycle (map op [:kill-zero :restart-zero]))])
        (when (:fix-alpha? opts)
          [(op :fix-alpha)])
        (when (:partition-halves? opts)
          [(cycle (map op [:start-partition-halves
                           :stop-partition-halves]))])
        (when (:partition-ring? opts)
          [(cycle (map op [:start-partition-ring
                           :stop-partition-ring]))])
        (when (:skew-clock? opts)
          [(cycle (map op [:start-skew :stop-skew]))])
        (when (:move-tablet? opts)
          [(op :move-tablet)])]
       (apply concat)
       gen/mix
       (gen/stagger (:interval opts))))

(defn nemesis
  "Composite nemesis and generator"
  [opts]
  {:nemesis   (full-nemesis opts)
   :generator (full-generator opts)
   :final-generator
   (->> [(when (:partition-halves? opts) :stop-partition-halves)
         (when (:partition-ring? opts)   :stop-partition-ring)
         (when (:skew-clock? opts)      :stop-skew)
         (when (:kill-zero?  opts)      :restart-zero)
         (when (:kill-alpha? opts)      :restart-alpha)]
        (remove nil?)
        (map op)
        (gen/delay-til 5))})
