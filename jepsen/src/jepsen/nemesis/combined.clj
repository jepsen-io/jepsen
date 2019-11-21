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
                    [util :as util :refer [majority
                                           random-nonempty-subset]]]))

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
     :all           - All nodes
     [\"a\", ...]   - The specified nodes"
  [test db node-spec]
  (let [nodes (:nodes test)]
    (case node-spec
      nil         (random-nonempty-subset nodes)
      :one        (list (rand-nth nodes))
      :minority   (take (dec (majority (count nodes))) (shuffle nodes))
      :majority   (take      (majority (count nodes))  (shuffle nodes))
      :primaries  (db/primaries db test)
      :all        nodes
      node-spec)))

(defn node-specs
  "Returns all possible node specification for the given DB. Helpful when you
  don't know WHAT you want to test."
  [db]
  (cond-> [nil :one :minority :majority :all]
    (satisfies? db/Primary db) (conj :primaries)))

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
  (let [{:keys [generator final-generator]} (db-generators opts)
        generator (gen/delay (:interval opts default-interval) generator)
        nemesis   (db-nemesis (:db opts))]
    {:generator       generator
     :final-generator final-generator
     :nemesis         nemesis
     :perf #{{:name        "kill"
              :start       #{:kill}
              :stop        #{:start}
              :fill-color  "#E9A4A0"}
             {:name        "pause"
              :start       #{:pause}
              :stop        #{:resume}
              :fill-color  "#A0B1E9"}}}))

(defn grudge
  "Computes a grudge from a partition spec. Spec may be one of:

    :one              Isolates a single node
    :majority         A clean majority/minority split
    :majorities-ring  Overlapping majorities in a ring"
  [test part-spec]
  (let [nodes (:nodes test)]
    (case part-spec
      :one              (n/complete-grudge (n/split-one nodes))
      :majority         (n/complete-grudge (n/bisect (shuffle nodes)))
      :majorities-ring  (n/majorities-ring nodes)
      part-spec)))

(defn partition-specs
  "All possible partition specs for a DB."
  [db]
  (cond-> [nil :one :majority :majorities-ring]
    (satisfies? db/Primary db) (conj :primaries)))

(defn partition-nemesis
  "Wraps a partitioner nemesis with support for partition specs."
  ([]
   (partition-nemesis (n/partitioner)))
  ([p]
   (reify
     n/Reflection
     (fs [this]
       [:start-partition :stop-partition])

     n/Nemesis
     (setup! [this test]
       (partition-nemesis (n/setup! p test)))

     (invoke! [this test op]
       (-> (case (:f op)
             ; Have the partitioner apply the calculated grudge.
             :start-partition (let [grudge (grudge test (:value op))]
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
  (let [targets (:targets (:partition opts) (partition-specs (:db opts)))
        start (fn [_ _] {:type  :info
                         :f     :start-partition
                         :value (rand-nth targets)})
        stop  {:type :info, :f :stop-partition, :value nil}
        gen   (->> (gen/flip-flop start stop)
                   (gen/delay (:interval opts default-interval)))]
    {:generator       gen
     :final-generator (gen/once stop)
     :nemesis         (partition-nemesis)
     :perf            #{{:name        "partition"
                         :start       #{:start-partition}
                         :stop        #{:stop-partition}
                         :fill-color  "#E9DCA0"}}}))

(defn compose-packages
  "Takes a collection of nemesis+generators packages and combines them into
  one. Generators are mixed together randomly; final generators proceed
  sequentially."
  [packages]
  {:generator       (gen/mix    (map  :generator packages))
   :final-generator (apply gen/concat (keep :final-generator packages))
   :nemesis         (n/compose  (map  :nemesis packages))
   :perf            (reduce into #{} (map :perf packages))})

(defn nemesis-package
  "Takes an option map, and returns a map with a :nemesis, a :generator for
  its operations, a :final-generator to clean up any failure modes at the end
  of a test, and a :perf map that can be passed to checker/perf to render nice
  graphs.

  Mandatory options:

    :db         The database you'd like to act on

  Optional options:

    :interval   The interval between operations, in seconds.
    :faults     A collection of enabled faults, e.g. [:partition, :kill, ...]
    :partition  Controls network partitions
    :kill       Controls process kills
    :pause      Controls process pauses and restarts

  Partition options:

    :targets    A collection of partition specs, e.g. [:majorities-ring, ...]

  Kill and Pause options:

    :targets    A collection of node specs, e.g. [:one, :all]"
  [opts]
  (let [faults   (set (:faults opts [:partition :kill :pause]))
        opts     (assoc opts :faults faults)
        packages (cond-> []
                   (faults :partition)          (conj (partition-package opts))
                   (some faults [:kill :pause]) (conj (db-package opts)))]
    (compose-packages packages)))
