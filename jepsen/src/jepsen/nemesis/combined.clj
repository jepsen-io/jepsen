(ns jepsen.nemesis.combined
  "A nemesis which combines common operations on nodes and processes: clock
  skew, crashes, pauses, and partitions. So far, writing these sorts of nemeses
  has involved lots of special cases. I expect that the API for specifying
  these nemeses is going to fluctuate as we figure out how to integrate those
  special cases appropriately. Consider this API unstable.

  This namespace introduces a new abstraction. A `nemesis+generator` is a map
  with a nemesis and a generator for that nemesis. This enables us to write an
  algebra for composing both simultaneously. We call
  checkers+generators+clients a \"workload\", but I don't have a good word for
  this except \"nemesis\". If you can think of a good word, please let me know.

  We also take advantage of the Process and Pause protocols in jepsen.db,
  which allow us to start, kill, pause, and resume processes."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [util :as util :refer [majority
                                           random-nonempty-subset]]]
            [jepsen.nemesis.time :as nt]))

(def default-interval
  "The default interval, in seconds, between nemesis operations."
  10)

(defn db-nodes
  "Takes a test, a DB, and a node specification. Returns a collection of
  nodes taken from that test. node-spec may be one of:

     nil            - Chooses a random, non-empty subset of nodes
     :one           - Chooses a single random node
     :minority      - Chooses a random minority of nodes
     :majority      - Chooses a random majority of nodes
     :primaries     - All nodes which we think are primaries
     :secondary     - Chooses a single random node that we don't think is primary
     :secondaries   - Chooses a random, non-empty subset of nodes we don't think are primaries
     :all           - All nodes
     [\"a\", ...]   - The specified nodes"
  [test db node-spec]
  (let [nodes (:nodes test)]
    (case node-spec
      nil          (random-nonempty-subset nodes)
      :one         (list (rand-nth nodes))
      :minority    (take (dec (majority (count nodes))) (shuffle nodes))
      :majority    (take      (majority (count nodes))  (shuffle nodes))
      :primaries   (db/primaries db test)
      :secondary   (list (rand-nth (db/secondaries db test)))
      :secondaries (util/random-nonempty-subset (db/secondaries db test))
      :all         nodes
      node-spec)))

(defn node-specs
  "Returns all possible node specification for the given DB. Helpful when you
  don't know WHAT you want to test."
  [db]
  (cond-> [nil :one :minority :majority :all]
    (satisfies? db/Primary db) (conj :primaries :follower :followers)))

(defn db-nemesis
  "A nemesis which can perform various DB-specific operations on nodes. Takes a
  database to operate on. This nemesis responds to the following f's:

     :start
     :kill
     :pause
     :resume

  In all cases, the :value is a node spec, as interpreted by db-nodes."
  [db]
  (reify
    n/Reflection
    (fs [this] #{:start :kill :pause :resume})

    n/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [f (case (:f op)
                :start   db/start!
                :kill    db/kill!
                :pause   db/pause!
                :resume  db/resume!)
            nodes (db-nodes test db (:value op))
            res (c/on-nodes test nodes (partial f db))]
        (assoc op :value res)))

    (teardown! [this test])))

(defn db-generators
  "A map with a :generator and a :final-generator for DB-related operations.
  Options are from nemesis-package."
  [opts]
  (let [db     (:db opts)
        faults (:faults opts)
        kill?  (and (satisfies? db/Process db) (contains? faults :kill))
        pause? (and (satisfies? db/Pause   db) (contains? faults :pause))

        ; Lists of possible specifications for nodes to kill/pause
        kill-targets  (:targets (:kill opts)  (node-specs db))
        pause-targets (:targets (:pause opts) (node-specs db))

        ; Starts and kills
        start  {:type :info, :f :start, :value :all}
        kill   (fn [_ _] {:type   :info
                          :f      :kill
                          :value  (rand-nth kill-targets)})

        ; Pauses and resumes
        resume {:type :info, :f :resume, :value :all}
        pause  (fn [_ _] {:type   :info
                          :f      :pause
                          :value  (rand-nth pause-targets)})

        ; Flip-flop generators
        kill-start   (gen/flip-flop kill start)
        pause-resume (gen/flip-flop pause resume)

        ; Automatically generate nemesis failure modes based on what the DB
        ; supports.
        db     (:db opts)
        modes  (cond-> []
                 pause? (conj pause-resume)
                 kill?  (conj kill-start))
        final  (cond-> []
                 pause? (conj resume)
                 kill?  (conj start))]
    {:generator       (gen/mix modes)
     :final-generator (gen/seq final)}))

(defn db-package
  "A nemesis and generator package for acting on a single DB. Options are from
  nemesis-package."
  [opts]
  (when (some #{:kill :pause} (:faults opts))
    (let [{:keys [generator final-generator]} (db-generators opts)
          generator (gen/delay (:interval opts default-interval) generator)
          nemesis   (db-nemesis (:db opts))]
      {:generator       generator
       :final-generator final-generator
       :nemesis         nemesis
       :perf #{{:name   "kill"
                :start  #{:kill}
                :stop   #{:start}
                :color  "#E9A4A0"}
               {:name   "pause"
                :start  #{:pause}
                :stop   #{:resume}
                :color  "#A0B1E9"}}})))

(defn grudge
  "Computes a grudge from a partition spec. Spec may be one of:

    :one              Isolates a single node
    :majority         A clean majority/minority split
    :majorities-ring  Overlapping majorities in a ring
    :primaries        Isolates all primaries into single-node components"
  [test db part-spec]
  (let [nodes (:nodes test)]
    (case part-spec
      :one              (n/complete-grudge (n/split-one nodes))
      :majority         (n/complete-grudge (n/bisect (shuffle nodes)))
      :majorities-ring  (n/majorities-ring nodes)
      :primaries        (let [primaries (db/primaries db test)]
                          (->> primaries
                               (map list) ; Put each in its own singleton list
                               (cons (remove (set primaries) nodes)) ; others
                               n/complete-grudge)) ; And make it a grudge

      part-spec)))

(defn partition-specs
  "All possible partition specs for a DB."
  [db]
  (cond-> [nil :one :majority :majorities-ring]
    (satisfies? db/Primary db) (conj :primaries)))

(defn partition-nemesis
  "Wraps a partitioner nemesis with support for partition specs. Uses db to
  determine primaries."
  ([db]
   (partition-nemesis db (n/partitioner)))
  ([db p]
   (reify
     n/Reflection
     (fs [this]
       [:start-partition :stop-partition])

     n/Nemesis
     (setup! [this test]
       (partition-nemesis db (n/setup! p test)))

     (invoke! [this test op]
       (-> (case (:f op)
             ; Have the partitioner apply the calculated grudge.
             :start-partition (let [grudge (grudge test db (:value op))]
                                (n/invoke! p test (assoc op
                                                         :f     :start
                                                         :value grudge)))
             ; Have the partitioner heal
             :stop-partition (n/invoke! p test (assoc op :f :stop)))
           ; Remap the :f to what the caller expects on the way back out
           (assoc :f (:f op))))

     (teardown! [this test]
       (n/teardown! p test)))))

(defn partition-package
  "A nemesis and generator package for network partitions. Options as for
  nemesis-package."
  [opts]
  (when ((:faults opts) :partition)
    (let [db      (:db opts)
          targets (:targets (:partition opts) (partition-specs db))
          start (fn [_ _] {:type  :info
                           :f     :start-partition
                           :value (rand-nth targets)})
          stop  {:type :info, :f :stop-partition, :value nil}
          gen   (->> (gen/flip-flop start stop)
                     (gen/delay (:interval opts default-interval)))]
      {:generator       gen
       :final-generator (gen/once stop)
       :nemesis         (partition-nemesis db)
       :perf            #{{:name  "partition"
                           :start #{:start-partition}
                           :stop  #{:stop-partition}
                           :color "#E9DCA0"}}})))

(defn clock-package
  "A nemesis and generator package for modifying clocks. Options as for
  nemesis-package."
  [opts]
  (when ((:faults opts) :clock)
    (let [nemesis (n/compose {{:reset-clock           :reset
                               :check-clock-offsets   :check-offsets
                               :strobe-clock          :strobe
                               :bump-clock            :bump}
                              (nt/clock-nemesis)})
          gen     (->> (nt/clock-gen)
                       (gen/f-map {:reset          :reset-clock
                                   :check-offsets  :check-clock-offsets
                                   :strobe         :strobe-clock
                                   :bump           :bump-clock})
                       (gen/delay (:interval opts default-interval)))]
      {:generator         gen
       :final-generator   (gen/once {:type :info, :f :reset-clock})
       :nemesis           nemesis
       :perf              #{{:name  "clock"
                             :start #{:bump-clock}
                             :stop  #{:reset-clock}
                             :fs    #{:strobe-clock}
                             :color "#A0E9E3"}}})))

(defn wan-nemesis
  "A nemesis which simulates running the cluster on a wide-area network, e.g.
  a cluster of nodes split across multiple different AWS regions, i.e. higher
  average latency, larger latency variance and spottier connectivity.

  This nemesis responds to the following f's:

      :drop     (Drop traffic between two nodes.)
      :slow     (Delays network packets.)
      :flaky    (Introduces randomzied packet loss.)
      :heal     (End all traffic drops and restore network to fast operation.)
      :fast     (Removes packet loss and delays.)"
  [_opts]
  (reify
    n/Reflection
    (fs [_this] #{:drop :slow :flaky :heal :fast})

    n/Nemesis
    (setup! [this test]
      (net/heal! (:net test) test)
      (net/fast! (:net test) test)
      this)

    (invoke! [this test op]
      (let [res (case (:f op)
                  :drop  (net/drop! (:net test) test (-> op :value :src) (-> op :value :dest))
                  :slow  (net/slow! (:net test) (assoc test :nodes (-> op :value :targets))
                                    {:mean         (-> op :value :mean)
                                     :variance     (-> op :value :variance)
                                     :distribution (-> op :value :distribution)})
                  :flaky (net/flaky! (:net test) (assoc test :nodes (-> op :value :targets)))
                  :heal  (net/heal!  (:net test) test)
                  :fast  (net/fast!  (:net test) test))]
        (assoc op :value res)))

    (teardown! [_this _test])))

(defn wan-generators
  "A map with a :generator and :final-generator for WAN-related operations
  Options are from nemesis-package."
  [opts]
  (let [db      (:db opts)
        faults  (:faults opts)
        drop?   (contains? faults :drop)
        slow?   (contains? faults :slow)
        flaky?  (contains? faults :flaky)

        ;; List of possible specifications for nodes to make slow or flaky. Note
        ;; that drop is special and already has a source and target node to target.
        slow-targets  (:targets (:slow  opts (node-specs db)))
        flaky-targets (:targets (:flaky opts (node-specs db)))

        ;; Drop and heal.
        drop' (fn [_ _]
                (let [nodes (-> opts :test :nodes)
                      src   (rand-nth nodes)
                      dest  (rand-nth (remove #{src} nodes))]
                  {:type :info
                   :f    :drop
                   :value {:src  src
                           :dest dest}}))
        heal {:type :info, :f :heal}
        drop-heal (gen/flip-flop drop' heal)

        ;; Slow and fast.
        slow (fn [test _process]
               (let [mean         50
                     variance     (rand-nth #{30 50 100 150 1000})
                     distribution :normal
                     target-specs (:targets (:slow opts) (node-specs db))
                     targets      (db-nodes test db
                                            (some-> target-specs seq rand-nth))]
                 {:type :info
                  :f     :slow
                  :value {:mean         mean
                          :variance     variance
                          :distribution distribution
                          :targets      targets}}))
        fast {:type :info, :f :fast}
        slow-fast (gen/flip-flop slow fast)

        ;; Flaky and fast.
        flaky (fn [test _process]
                (let [target-specs (:targets (:flaky opts) (node-specs db))
                      targets (db-nodes test db
                                        (some-> target-specs seq rand-nth))]
                  {:type :info
                   :f    :flaky
                   :value {:targets targets}}))
        flaky-fast (gen/flip-flop flaky fast)

        modes (cond-> []
                drop?  (conj drop-heal)
                slow?  (conj slow-fast)
                flaky? (conj flaky-fast))

        final (cond-> []
                drop?             (conj heal)
                (or slow? flaky?) (conj fast))]

    {:generator       (gen/mix modes)
     :final-generator (gen/seq final)}))

(defn wan-package
  "A nemesis and generator package for Wide-area network simulation operations.
  Options are from nemesis-package."
  [opts]
  (when (some #{:drop :slow :flaky} (:faults opts))
    (let [{:keys [generator final-generator]} (wan-generators opts)
          generator (gen/delay (:interval opts default-interval) generator)
          nemesis   (wan-nemesis opts)]
      {:generator       generator
       :final-generator final-generator
       :nemesis         nemesis
       :perf #{{:name   "drop"
                :start  #{:drop}
                :stop   #{:heal}
                :color  "#00FF00"}
               {:name   "slow"
                :start  #{:slow}
                :stop   #{:fast}
                :color  "#FF00FF"}
               {:name   "flaky"
                :start  #{:flaky}
                :stop   #{:fast}
                :color  "#FFFF00"}}})))

(defn compose-packages
  "Takes a collection of nemesis+generators packages and combines them into
  one. Generators are mixed together randomly; final generators proceed
  sequentially."
  [packages]
  {:generator       (gen/mix    (map  :generator packages))
   :final-generator (apply gen/concat (keep :final-generator packages))
   :nemesis         (n/compose  (map  :nemesis packages))
   :perf            (reduce into #{} (map :perf packages))})

(defn nemesis-packages
  "Just like nemesis-package, but returns a collection of packages, rather than
  the combined package, so you can manipulate it further before composition."
  [opts]
  (let [faults   (set (:faults opts [:partition :kill :pause :clock :drop :slow :flaky]))
        opts     (assoc opts :faults faults)]
    (remove nil? [(partition-package opts)
                  (clock-package opts)
                  (db-package opts)
                  (wan-package opts)])))

(defn nemesis-package
  "Takes an option map, and returns a map with a :nemesis, a :generator for
  its operations, a :final-generator to clean up any failure modes at the end
  of a test, and a :perf map that can be passed to checker/perf to render nice
  graphs.

  This nemesis is intended for throwing a broad array of simple failures at the
  wall, and seeing \"what sticks\". Once you've found a fault, you can restrict
  the failure modes to specific types of faults, and specific targets for those
  faults, to try and reproduce it faster.

  This nemesis is *not* intended for complex sequences of faults, like
  partitionining away a leader, flipping some switch, adjusting the clock on an
  unrelated node, then crashing someone else. I don't think I can devise a good
  declarative langauge for that in a way which is simpler than \"generators\"
  themselves. For those types of faults, you'll write your own generator
  instead, but you may be able to use this *nemesis* to execute some or all of
  those operations.

  Mandatory options:

    :db         The database you'd like to act on

  Optional options:

    :interval   The interval between operations, in seconds.
    :faults     A collection of enabled faults, e.g. [:partition, :kill, ...]
    :partition  Controls network partitions
    :kill       Controls process kills
    :pause      Controls process pauses and restarts
    :slow       Controls slowing down of the network
    :flaky      Controls introduction of packet loss in the network

  Possible faults:

    :partition
    :kill
    :pause
    :clock
    :drop
    :slow
    :flaky

  Partition options:

    :targets    A collection of partition specs, e.g. [:majorities-ring, ...]

  Kill, Pause, Slow and Flaky options:

    :targets    A collection of node specs, e.g. [:one, :all]"
  [opts]
  (compose-packages (nemesis-packages opts)))
