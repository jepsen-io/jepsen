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
            [clojure [set :as set]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [nemesis :as n]
                    [net :as net]
                    [util :as util :refer [majority
                                           minority-third
                                           rand-distribution
                                           random-nonempty-subset]]]
            [jepsen.nemesis.time :as nt]))

(def default-interval
  "The default interval, in seconds, between nemesis operations."
  10)

(def noop
  "A package which does nothing."
  {:generator       nil
   :final-generator nil
   :nemesis         n/noop
   :perf            #{}})

(defn db-nodes
  "Takes a test, a DB, and a node specification. Returns a collection of
  nodes taken from that test. node-spec may be one of:

     nil              - Chooses a random, non-empty subset of nodes
     :one             - Chooses a single random node
     :minority        - Chooses a random minority of nodes
     :majority        - Chooses a random majority of nodes
     :minority-third  - Up to, but not including, 1/3rd of nodes
     :primaries       - A random nonempty subset of nodes which we think are
                        primaries
     :all             - All nodes
     [\"a\", ...]     - The specified nodes"
  [test db node-spec]
  (let [nodes (:nodes test)]
    (case node-spec
      nil         (random-nonempty-subset nodes)
      :one        (list (rand-nth nodes))
      :minority   (take (dec (majority (count nodes))) (shuffle nodes))
      :majority   (take      (majority (count nodes))  (shuffle nodes))
      :minority-third (take (minority-third (count nodes)) (shuffle nodes))
      :primaries  (random-nonempty-subset (db/primaries db test))
      :all        nodes
      node-spec)))

(defn node-specs
  "Returns all possible node specification for the given DB. Helpful when you
  don't know WHAT you want to test."
  [db]
  (cond-> [nil :one :minority-third :minority :majority :all]
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
        kill-start (gen/flip-flop kill (repeat start))
        pause-resume (gen/flip-flop pause (repeat resume))

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
     :final-generator final}))

(defn db-package
  "A nemesis and generator package for acting on a single DB. Options are from
  nemesis-package."
  [opts]
  (let [needed? (some #{:kill :pause} (:faults opts))
        {:keys [generator final-generator]} (db-generators opts)
        generator (gen/stagger (:interval opts default-interval)
                               generator)
        nemesis   (db-nemesis (:db opts))]
    {:generator       (when needed? generator)
     :final-generator (when needed? final-generator)
     :nemesis         nemesis
     :perf #{{:name   "kill"
              :start  #{:kill}
              :stop   #{:start}
              :color  "#E9A4A0"}
             {:name   "pause"
              :start  #{:pause}
              :stop   #{:resume}
              :color  "#A0B1E9"}}}))

(defn grudge
  "Computes a grudge from a partition spec. Spec may be one of:

    :one              Isolates a single node
    :majority         A clean majority/minority split
    :majorities-ring  Overlapping majorities in a ring
    :minority-third   Cleanly splits away up to, but not including, 1/3rd of
                      nodes
    :primaries        Isolates a nonempty subset of primaries into
                      single-node components"
  [test db part-spec]
  (let [nodes (:nodes test)]
    (case part-spec
      :one              (n/complete-grudge (n/split-one nodes))
      :majority         (n/complete-grudge (n/bisect (shuffle nodes)))
      :majorities-ring  (n/majorities-ring nodes)
      :minority-third   (n/complete-grudge (split-at (util/minority-third
                                                       (count nodes))
                                                     (shuffle nodes)))
      :primaries        (let [primaries (db/primaries db test)]
                          (->> primaries
                               random-nonempty-subset
                               (map list) ; Put each in its own singleton list
                               (cons (remove (set primaries) nodes)) ; others
                               n/complete-grudge)) ; And make it a grudge

      part-spec)))

(defn partition-specs
  "All possible partition specs for a DB."
  [db]
  (cond-> [:one :minority-third :majority :majorities-ring]
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
  (let [needed? ((:faults opts) :partition)
        db      (:db opts)
        targets (:targets (:partition opts) (partition-specs db))
        start (fn start [_ _]
                {:type  :info
                 :f     :start-partition
                 :value (rand-nth targets)})
        stop  {:type :info, :f :stop-partition, :value nil}
        gen   (->> (gen/flip-flop start (repeat stop))
                   (gen/stagger (:interval opts default-interval)))]
    {:generator       (when needed? gen)
     :final-generator (when needed? stop)
     :nemesis         (partition-nemesis db)
     :perf            #{{:name  "partition"
                         :start #{:start-partition}
                         :stop  #{:stop-partition}
                         :color "#E9DCA0"}}}))

(defn packet-nemesis
  "A nemesis to disrupt packets, e.g. delay, loss, corruption, etc.
   Takes a db to work with [[db-nodes]].

   The network behavior is applied to all traffic to and from the target nodes.
   
  This nemesis responds to:
  ```
  {:f :start-packet :value [:node-spec   ; target nodes as interpreted by db-nodes
                            {:delay {},  ; behaviors that disrupt packets
                             :loss  {:percent :33%},...}]} 
  {:f :stop-packet  :value nil}
   ```
  See [[jepsen.net/all-packet-behaviors]]."
  [db]
  (reify
    n/Reflection
    (fs [_this]
      [:start-packet  :stop-packet])

    n/Nemesis
    (setup! [this {:keys [net] :as test}]
      ; start from known good state, no shaping
      (net/shape! net test nil nil)
      this)

    (invoke! [_this {:keys [net] :as test} {:keys [f value] :as op}]
      (let [result (case f
                     :start-packet (let [[targets behaviors] value
                                         targets (db-nodes test db targets)]
                                     (net/shape! net test targets behaviors))
                     :stop-packet  (net/shape! net test nil nil))]
        (assoc op :value result)))

    (teardown! [_this {:keys [net] :as test}]
       ; leave in known good state, no shaping
      (net/shape! net test nil nil))))

(defn packet-package
  "A nemesis and generator package that disrupts packets,
   e.g. delay, loss, corruption, etc.
   
   Opts:
   ```clj
   {:packet
    {:targets      ; A collection of node specs, e.g. [:one, :all]
     :behaviors [  ; A collection of network behaviors that disrupt packets, e.g.:
      {}                         ; no disruptions
      {:delay {}}                ; delay packets by default amount
      {:corrupt {:percent :33%}} ; corrupt 33% of packets
      ; delay packets by default values, plus duplicate 25% of packets
      {:delay {},
       :duplicate {:percent :25% :correlation :80%}}]}}
  ```
  See [[jepsen.net/all-packet-behaviors]].

  Additional options as for [[nemesis-package]]."
  [opts]
  (let [needed?   ((:faults opts) :packet)
        db        (:db opts)
        targets   (:targets   (:packet opts) (node-specs db))
        behaviors (:behaviors (:packet opts) [{}])
        start     (fn start [_ _]
                    {:type  :info
                     :f     :start-packet
                     :value [(rand-nth targets) (rand-nth behaviors)]})
        stop      {:type  :info
                   :f     :stop-packet
                   :value nil}
        gen       (->> (gen/flip-flop start (repeat stop))
                       (gen/stagger (:interval opts default-interval)))]
    {:generator       (when needed? gen)
     :final-generator (when needed? stop)
     :nemesis         (packet-nemesis db)
     :perf            #{{:name  "packet"
                         :start #{:start-packet}
                         :stop  #{:stop-packet}
                         :color "#D1E8A0"}}}))

(defn clock-package
  "A nemesis and generator package for modifying clocks. Options as for
  nemesis-package."
  [opts]
  (let [needed? ((:faults opts) :clock)
        nemesis (n/compose {{:reset-clock           :reset
                             :check-clock-offsets   :check-offsets
                             :strobe-clock          :strobe
                             :bump-clock            :bump}
                            (nt/clock-nemesis)})
        db (:db opts)
        target-specs (:targets (:clock opts) (node-specs db))
        targets (fn [test] (db-nodes test db
                                     (some-> target-specs seq rand-nth)))
        clock-gen (gen/phases
                    {:type :info, :f :check-offsets}
                    (gen/mix [(nt/reset-gen-select  targets)
                              (nt/bump-gen-select   targets)
                              (nt/strobe-gen-select targets)]))
        gen (->> clock-gen
                 (gen/f-map {:reset          :reset-clock
                             :check-offsets  :check-clock-offsets
                             :strobe         :strobe-clock
                             :bump           :bump-clock})
                 (gen/stagger (:interval opts default-interval)))]
    {:generator         (when needed? gen)
     :final-generator   (when needed? {:type :info, :f :reset-clock})
     :nemesis           nemesis
     :perf              #{{:name  "clock"
                           :start #{:bump-clock}
                           :stop  #{:reset-clock}
                           :fs    #{:strobe-clock}
                           :color "#A0E9E3"}}}))

(defn file-corruption-nemesis
  "Wraps [[jepsen.nemesis/bitflip]] and [[jepsen.nemesis/truncate-file]] to corrupt files.
   
   Responds to:
   ```
   {:f :bitflip  :value [:node-spec ... ; target nodes as interpreted by db-nodes
                         {:file \"/path/to/file/or/dir\" :probability 1e-5}]} 
   {:f :truncate :value [:node-spec ... ; target nodes as interpreted by db-nodes
                         {:file \"/path/to/file/or/dir\" :drop {:distribution :geometric :p 1e-3}}]} 
   ```
  See [[jepsen.nemesis.combined/file-corruption-package]]."
  ([db] (file-corruption-nemesis db (n/bitflip) (n/truncate-file)))
  ([db bitflip truncate]
  (reify
    n/Reflection
    (fs [_this]
      [:bitflip :truncate])

    n/Nemesis
    (setup! [_this test]
      (file-corruption-nemesis db (n/setup! bitflip test) (n/setup! truncate test)))

    (invoke! [_this test {:keys [f value] :as op}]
      (let [[node-spec corruption] value
            targets (db-nodes test db node-spec)
            plan    (->> targets
                         (reduce (fn [plan node]
                                   (assoc plan node corruption))
                                 {}))
            op      (assoc op :value plan)]
        (case f
          :bitflip  (n/invoke! bitflip  test op)
          :truncate (n/invoke! truncate test op))))

    (teardown! [this test]
      (n/teardown! bitflip  test)
      (n/teardown! truncate test)
      this))))

(defn file-corruption-package
  "A nemesis and generator package that corrupts files.
   
   Opts:
   ```clj
   {:file-corruption
    {:targets     [...] ; A collection of node specs, e.g. [:one, [\"n1\", \"n2\"], :all]
     :corruptions [     ; A collection of file corruptions, e.g.:
      {:type :bitflip
       :file \"/path/to/file\"
       :probability 1e-3},
      {:type :bitflip
       :file \"path/to/dir\"
       :probability {:distribution :one-of :values [1e-3 1e-4 1e-5]}},
      {:type :truncate
       :file \"path/to/file/or/dir\"
       :drop {:distribution :geometric :p 1e-3}}]}}
   ```
   
   `:type` can be `:bitflip` or `:truncate`.

   If `:file` is a directory,
   a new random file is selected from that directory on each target node for each operation.

   `:probability` or `:drop` can be specified as a single value or a `distribution-map`.
   Use a `distribution-map` to generate a new random value for each operation using [[jepsen.util/rand-distribution]].
   
   See [[jepsen.nemesis/bitflip]] and [[jepsen.nemesis/truncate-file]].

   Additional options as for [[nemesis-package]]."
  [{:keys [faults db file-corruption interval] :as _opts}]
  (let [needed?     (:file-corruption faults)
        targets     (:targets     file-corruption (node-specs db))
        corruptions (:corruptions file-corruption)
        gen (->> (fn gen [_test _context]
                   (let [target (rand-nth targets)
                         {:keys [type file probability drop]} (rand-nth corruptions)
                         corruption (case type
                                      :bitflip  (let [probability (cond
                                                                    (number? probability) probability
                                                                    (map? probability) (rand-distribution probability))]
                                                  {:file file :probability probability})
                                      :truncate (let [drop (cond
                                                             (number? drop) drop
                                                             (map? drop) (rand-distribution drop))]
                                                  {:file file :drop drop}))]
                     {:type  :info
                      :f     type
                      :value [target corruption]}))
                 (gen/stagger (or interval default-interval)))]
    {:generator (when needed? gen)
     :nemesis   (file-corruption-nemesis db)
     :perf      #{{:name  "file-corruption"
                   :fs    #{:bitflip :truncate}
                   :start #{}
                   :stop  #{}
                   :color "#99F2E2"}}}))

(defn f-map-perf
  "Takes a perf map, and transforms the fs in it using `lift`."
  [lift perf]
  (let [lift-set (comp set (partial map lift))]
    (set (map (fn [perf]
                (cond-> perf
                  true          (update :name  lift)
                  (:start perf) (update :start lift-set)
                  (:stop perf)  (update :stop  lift-set)
                  (:fs perf)    (update :fs    lift-set)))
              perf))))

(defn f-map
  "Takes a function `lift` which (presumably injectively) transforms the :f
  values used in operations, and a nemesis package. Yields a new nemesis
  package which uses the lifted fs. See generator/f-map and nemesis/f-map."
  [lift pkg]
  (assoc pkg
         :generator       (gen/f-map  lift (:generator pkg))
         :final-generator (gen/f-map  lift (:final-generator pkg))
         :nemesis         (n/f-map    lift (:nemesis pkg))
         :perf            (f-map-perf lift (:perf pkg))))

(defn compose-packages
  "Takes a collection of nemesis+generators packages and combines them into
  one. Generators are combined with gen/any. Final generators proceed
  sequentially."
  [packages]
  (case (count packages)
    0 noop
    1 (first packages)
    {:generator       (apply gen/any (keep :generator packages))
     :final-generator (keep :final-generator packages)
     :nemesis         (n/compose (keep :nemesis packages))
     :perf            (reduce set/union (map :perf packages))}))

(defn nemesis-packages
  "Just like nemesis-package, but returns a collection of packages, rather than
  the combined package, so you can manipulate it further before composition."
  [opts]
  (let [faults   (set (:faults opts [:partition :packet :kill :pause :clock :file-corruption]))
        opts     (assoc opts :faults faults)]
    [(partition-package opts)
     (packet-package opts)
     (file-corruption-package opts)
     (clock-package opts)
     (db-package opts)]))

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
    :packet     Controls network packet behavior
    :kill       Controls process kills
    :pause      Controls process pauses and restarts
    :file-corruption Controls file corruption

  Possible faults:

    :partition
    :packet
    :kill
    :pause
    :clock
    :file-corruption

  Partition options:

    :targets    A collection of partition specs, e.g. [:majorities-ring, ...]
  
  Packet options:
    
    :targets    A collection of node specs, e.g. [:one, :all]
    :behaviors  A collection of network packet behaviors, e.g. [{:delay {}}]
    
  Kill and Pause options:

    :targets    A collection of node specs, e.g. [:one, :all]

  File corruption options:
    
    :targets     A collection of node specs, e.g. [:one, :all]
    :corruptions A collection of file corruptions, e.g. [{:type :bitflip, :file \"/path/to/file\" :probability 1e-3}]"
  [opts]
  (compose-packages (nemesis-packages opts)))
