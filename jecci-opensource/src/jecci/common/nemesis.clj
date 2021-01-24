(ns jecci.common.nemesis
  "Nemeses for jecci"
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nemesis]
             [net :as net]
             [generator :as gen]
             [util :as util :refer [letr]]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.time :as nt]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jecci.common.db :as db]
            [clojure.tools.logging :refer :all]
            [slingshot.slingshot :refer [try+ throw+]]
            [jecci.interface.nemesis :as in :refer :all]
            [jecci.interface.db :as idb]))

(defn full-nemesis
  "Merges together all nemeses"
  []
  (nemesis/compose
    (merge
      in/nemesis-composition
      {{:start-partition :start
       :stop-partition  :stop}               (nemesis/partitioner nil)
      ;{:reset-clock          :reset
      ; :strobe-clock         :strobe
      ; :check-clock-offsets  :check-offsets
      ; :bump-clock           :bump}          (nt/clock-nemesis)
      }
      )))

; Generators

(defn op
  "Shorthand for constructing a nemesis op"
  ([f]
   (op f nil))
  ([f v]
   {:type :info, :f f, :value v})
  ([f v & args]
   (apply assoc (op f v) args)))

(defn partition-one-gen
  "A generator for a partition that isolates one node."
  [test process]
  (op :start-partition
    (->> test :nodes nemesis/split-one nemesis/complete-grudge)
    :partition-type :single-node))

(defn partition-half-gen
  "A generator for a partition that cuts the network in half."
  [test process]
  (op :start-partition
    (->> test :nodes shuffle nemesis/bisect nemesis/complete-grudge)
    :partition-type :half))

(defn partition-ring-gen
  "A generator for a partition that creates overlapping majority rings"
  [test process]
  (op :start-partition
    (->> test :nodes nemesis/majorities-ring)
    :partition-type :ring))

(defn clock-gen
  "A mixture of clock operations."
  []
  (->> (nt/clock-gen)
    (gen/f-map {:check-offsets  :check-clock-offsets
                :reset          :reset-clock
                :strobe         :strobe-clock
                :bump           :bump-clock})))


(defn opt-mix
  "Given a nemesis map n, and a map of options to generators to use if that
  option is present in n, constructs a mix of generators for those options. If
  no options match, returns `nil`."
  [n possible-gens]
  (let [gens (reduce (fn [gens [option gen]]
                       (if (option n)
                         (conj gens gen)
                         gens))
                     []
                     possible-gens)]
    (when (seq gens)
      (gen/mix gens))))

(defrecord Mix-Dup [i trigger gens]
  ; i is the next generator index we intend to work with; we reset it randomly
  ; when emitting ops.
  gen/Generator
  (op [_ test ctx]
    (when (seq gens)
      (if-let [[op gen'] (gen/op (nth gens i) test ctx)]
        ; Good, we have an op
        [op (Mix-Dup. (if (= trigger 0) (rand-int (count gens)) i) (bit-xor trigger 1) (assoc gens i gen'))]
        ; Oh, we're out of ops on this generator. Compact and recur.
        (op (Mix-Dup. (rand-int (dec (count gens))) 1 (gen/dissoc-vec gens i))
            test ctx))))

  (update [this test ctx event]
    this))

(defn mix-dup
  [gens]
  (Mix-Dup. (rand-int (count gens)) 1 (vec gens)))

(defn mixed-generator
  "Takes a nemesis options map `n`, and constructs a generator for all nemesis
  operations. This generator is used during normal nemesis operations."
  [n]
  ; Shorthand: we're going to have a bunch of flip-flops with various types of
  ; failure conditions and a single recovery.
  (let [o (fn [possible-gens recovery]
            ; We return nil when mix does to avoid generating flip flops when
            ; *no* options are present in thclock-gene nemesis opts.
            (when-let [mix (opt-mix n possible-gens)]
              (gen/flip-flop (gen/repeat mix) (gen/repeat recovery))))
        p (fn [nem-sym nem recovery]
            (when (nem-sym n)
              (gen/flip-flop (gen/repeat nem) (gen/repeat recovery))))
        ks (into #{:partition-one
                   :partition-half
                   :partition-ring}
                 (concat (keys in/op2heal-dict) (keys in/autorecover-ops)))
        impls (reduce into #{:partition-one
                      :partition-half
                      :partition-ring}
                    (keys in/nemesis-composition))
        nems (filter #(not (contains? #{:interval :long-recovery :schedule} %))
                     (keys n))
        _ (when-not (clojure.set/subset? (into #{} nems) ks)
            (error (str "You are using nemeses: " nems
                       " But jecci cannot find one of them in op2heal-dict and autorecover-ops."
                        " This means jepsen will not generate any operation of the undefined nemesis.")))
        _ (when-not (clojure.set/subset? (into #{} nems) impls)
            (error (str "You are using nemeses: " nems
                        " But jecci cannot find one of them in nemesis-composition. This means "
                        "jepsen will not be able to run a function that can understand what to do with the undefined nemesis")))]

    ; Mix together our different types of process crashes, partitions, and
    ; clock skews.

    (->>
      [(o {:partition-one        partition-one-gen
            :partition-half       partition-half-gen
            :partition-ring       partition-ring-gen}
            (op :stop-partition))

          (opt-mix n {:clock-skew ()})]
      (into (into [] (map (fn [kv] (o {(key kv) (op (key kv))} (op (val kv)))) in/op2heal-dict)))
      ; todo: add opt-mix to autorecover-ops
      (into (into [] (map (fn [kv] (p (key kv) (op (key kv)) (op (val kv)))) in/autorecover-ops)))
      ; For all options relevant for this nemesis, mix them together
      (remove nil?)
      mix-dup
      ; Introduce either random or fixed delays between ops
      (
       ;(case (:schedule n)
       ;  (nil :random)    gen/stagger
       ;  :fixed           gen/delay-til) :fixed is disabled for now since
       ; delay-til is gone in jepsen 2.0
       gen/stagger
       (:interval n)))))

(defn final-generator
  "Takes a nemesis options map `n`, and constructs a generator to stop all
  problems. This generator is called at the end of a test, before final client
  operations."
  [n]
  (->> (cond-> []
         (:clock-skew n)      (conj :reset-clock)
         (some n [:partition-one :partition-half :partition-ring]) (conj :stop-partition))
    (into (into [] (vals (filter (fn [[k v]] (k n)) in/final-gen-dict))))
    (map op)
    (map gen/once)))

(defn slow-primary-generator
  "A special generator which tries to create a situation in which a primary,
  running slower than the rest of the cluster, is isolated from a majority
  component of the cluster, which elects a new, faster primary. Because the old
  primary's clock runs slow, we expect that the slow node may fail to step down
  before the new primary comes to power, allowing the two to issue timestamps
  concurrently."
  []
  ; First, pick a node to be our slow primary.
  ; Force that node to
  ; run at speed 2.
  ; Make that node run at speed 2
  ; Force that node to be the primary by...
  ; Restarting every other node at speed 1
  ; Force that node to be slow *and* the leader by...
  ; Restarting every other node at speed 3
  ; Isolate that node into a minority partition
  (->> [{:type :info, :f :slow-primary}
        (gen/sleep 30)
        {:type :info, :f :stop-partition}
        (gen/sleep 30)]
    cycle
    (map gen/once)))

(defn full-generator
  "Takes a nemesis options map `n`. If `n` has a :long-recovery option, builds
  a generator which alternates between faults (mixed-generator) and long
  recovery windows (final-generator). Otherwise, just emits faults from
  mixed-generator, or whatever special-case generator we choose."
  [n]
  (let [special-gen (in/special-full-generator n)]
    (info "using nemesis: " n)
    (if special-gen
      special-gen
      (cond
            (:slow-primary n)
            (slow-primary-generator)

            (:long-recovery n)
            (let [mix     #(gen/time-limit 120 (mixed-generator n))
                  recover #(gen/phases (final-generator n)
                             (gen/sleep 60))]
              (interleave (repeatedly mix)
                (repeatedly recover)))

            true
            (mixed-generator n)))))


(defn nemesis
  "Composite nemesis and generator, given test options."
  [opts]
  (let [n (:nemesis opts)]
    {:nemesis         (full-nemesis)
     :generator       (full-generator n)
     :final-generator (final-generator n)}))
