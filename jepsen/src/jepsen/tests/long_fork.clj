(ns jepsen.tests.long-fork
  "Tests for an anomaly in parallel snapshot isolation (but which is prohibited
  in normal snapshot isolation). In long-fork, concurrent write transactions
  are observed in conflicting order. For example:

  T1: (write x 1)
  T2: (write y 1)
  T3: (read x nil) (read y 1)
  T4: (read x 1) (read y nil)

  T3 implies T2 < T1, but T4 implies T1 < T2. We aim to observe these
  conflicts.

  To generalize to multiple updates...

  Let a write transaction be a transaction of the form [(write k v)], executing
  a single write to a single register. Assume no two write transactions have
  the same key *and* value.

  Let a read transaction be a transaction of the form [(read k1 v1) (read k2
  v2) ...], reading multiple distinct registers.

  Let R be a set of reads, and W be a set of writes. Let the total set of
  transactions T = R U W; e.g. there are only reads and writes. Let the initial
  state be empty.

  Serializability implies that there should exist an order < over T, such that
  every read observes the state of the system as given by the prefix of all
  write transactions up to some point t_i.

  Since each value is written exactly once, a sequence of states with the same
  value for a key k must be *contiguous* in this order. That is, it would be
  illegal to read:

      [(r x 0)] [(r x 1)] [(r x 0)]

  ... because although we can infer (w x 0) happened before t1, and (w x 1)
  happened between t2, there is no *second* (w x 0) that can satisfy t3.
  Visually, imagine bars which connect identical values from reads, keeping
  them together:

      key   r1    r2    r3    r4

      x     0     1 --- 1 --- 1

      y     0     1 --- 1     0

      z     0 --- 0     2     1

  A long fork anomaly manifests when we cannot construct an order where these
  bars connect identical values into contiguous blocks.

  Note that more than one value may change between reads: we may not observe
  every change.

  Note that the values for a given key are not monotonic; we must treat them as
  unordered, opaque values because the serialization order of writes is (in
  general) arbitrary. It would be easier if we knew the write order up front.
  The classic example of long fork uses inserts because we know the states go
  from `nil` to `some-value`, but in our case, it could be 0 -> 1, or 1 -> 0.

  To build a graph like this, we need an incremental process. We know the
  initial state was [nil nil nil ...], so we can start there. Let our sequence
  be:

          r0

      x   nil
      y   nil
      z   nil

  We know that values can only change *away* from nil. This implies that those
  reads with the most nils must come adjacent to r0. In general, if there
  exists a read r1 adjacent to r0, then there must not exist any read r2 such
  that r2 has more in common with r0 than r1. This assumes that all reads are
  total.

  Additionally, if the graph is to be... smooth, as opposed to forking, there
  are only two directions to travel from any given read. This implies there can
  be at most two reads r1 and r2 with an equal number of links *and* distinct
  links to r0. If a third read with this property exists, there's 'nowhere to
  put it' without creating a fork.

  We can verify this property in roughly linear time, which is nice. It
  doesn't, however, prevent *closed loops* with no forking structure.

  To do loops, I think we have to actually do the graph traversal. Let's punt
  on that for now."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [generator :as gen]
                    [checker :as checker]
                    [history :as h]
                    [util :refer [rand-nth-empty]]]
            [jepsen.txn.micro-op :as mop]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn group-for
  "Takes a key and returns the collection of keys for its group.
  Lower inclusive, upper exclusive."
  [n k]
  (let [m (mod k n)
        l (- k m)
        u (+ l n)]
    (range l u)))

(defn read-txn-for
  "Takes a group size and a key and generates a transaction reading that key's
  group in shuffled order."
  [n k]
  (->> (group-for n k)
       shuffle
       (mapv (fn [k] [:r k nil]))))

; n is the group size.
; next-key is the next free key we have to allocate.
; workers is a map of worker numbers to the last key that worker wrote.
(defrecord Generator [n next-key workers]
  gen/Generator
  (update [this test ctx event] this)

  (op [this test ctx]
    ; If a worker's last written key is nil, it can either issue a read for a
    ; write in progress, or it can perform a write itself. To write, it
    ; generates a fresh integer from next-key. and writes it, recording that
    ; key in the workers map. If it *is* present in the map, it issues a read
    ; to the group covering that key.
    (let [process (gen/some-free-process ctx)
          worker  (gen/process->thread ctx process)]
      (if worker
        (if-let [k (get workers worker)]
          ; We wrote a key; produce a read and clear our last written key.
          [(gen/fill-in-op
             {:process process, :f :read, :value (read-txn-for n k)}
             ctx)
           (Generator. n next-key (assoc workers worker nil))]
          ; OK we didn't wite a key--let's try one of two random options.
          (if-let [k (and (< (rand) 0.5)
                          (rand-nth-empty (keep val workers)))]
            ; Read some other active group
            [(gen/fill-in-op
               {:process process, :f :read, :value (read-txn-for n k)}
               ctx)
             this]

            ; Write a fresh key
            [(gen/fill-in-op
               {:process process, :f :write, :value [[:w next-key 1]]}
               ctx)
             (Generator. n (inc next-key) (assoc workers worker next-key))]))
        [:pending this]))))

(defn generator
  "Generates single inserts followed by group reads, mixed with reads of other
  concurrent groups, just for grins. Takes a group size n."
  [n]
  (Generator. n 0 {}))

(defn read-compare
  "Given two maps of keys to values, a and b, returns -1 if a dominates, 0 if
  the two are equal, 1 if b dominates, or nil if a and b are incomparable."
  [a b]
  (when (not= (count a) (count b))
    (throw+ {:type :illegal-history
             :reads [a b]
             :msg "These reads did not query for the same keys, and therefore cannot be compared."}))

  (reduce (fn [res k]
            (let [va (get a k)
                  vb (get b k ::not-found)]
              (cond ; Different key sets
                    (= ::not-found vb)
                    (throw+ {:type :illegal-history
                             :reads [a b]
                             :key   k
                             :msg "These reads did not query for the same keys, and therefore cannot be compared."})

                    ; Both equal
                    (= va vb) res

                    ; A bigger here
                    (nil? vb) (if (pos? res)
                                (reduced nil)
                                -1)

                    ; B bigger here
                    (nil? va) (if (neg? res)
                                (reduced nil)
                                1)

                    ; Different values, both present; this is illegal
                    true (throw+ {:type  :illegal-history
                                  :key   k
                                  :msg  "These two read states contain distinct values for the same key; this checker assumes only one write occurs per key."
                                  :reads [a b]}))))
          0
          (keys a)))

(defn read-op->value-map
  "Takes a read operation, and converts it to a map of keys to values."
  [op]
  (loop [ops (seq (:value op))
         m   (transient {})]
    (if-not ops
      (persistent! m)
      (let [[f k v] (first ops)]
        (recur (next ops) (assoc! m k v))))))

(defn distinct-pairs
  "Given a collection, returns a sequence of all unique 2-element sets taken
  from that collection."
  [coll]
  (->> (for [a coll, b coll :when (not= a b)] #{a b})
       distinct
       (map vec)))

(defn find-forks
  "Given a set of read ops, compares every one to ensure a total order exists.
  If mutually incomparable reads exist, returns the pair."
  [ops]
  (->> (distinct-pairs ops)
       (keep (fn [[a b]]
               (when (nil? (read-compare (read-op->value-map a)
                                         (read-op->value-map b)))
                 [a b])))))

(defn read-txn?
  "Is this transaction a pure read txn?"
  [txn]
  (every? mop/read? txn))

(defn write-txn?
  "Is this a pure write transaction?"
  [txn]
  (and (= 1 (count txn))
       (mop/write? (first txn))))

(defn legal-txn?
  "Checks to ensure a txn is legal."
  [txn]
  (or (read-txn? txn)
      (write-txn? txn)))

(defn op-read-keys
  "Given a read op, returns the set of keys read."
  [op]
  (->> op :value (map mop/key) set))

(defn groups
  "Given a group size n, and a set of read ops, partitions those read
  operations by group. Throws if any group has the wrong size."
  [n read-ops]
  (reduce (fn [groups [group ops]]
            (when (not= n (count group))
              (throw+ {:type :illegal-history
                       :op  (first ops)
                       :msg (str "Every read in this history should have observed exactly "
                                 n " keys, but this read observed "
                                 (count group) " instead: " (pr-str group))}))
            (conj groups ops))
          []
          (group-by op-read-keys read-ops)))

(defn ensure-no-long-forks
  "Returns a checker error if any long forks exist."
  [n reads]
  (let [forks (->> reads
                   (groups n)
                   (mapcat find-forks))]
    (when (seq forks)
      {:valid? false
       :forks  forks})))

(defn ensure-no-multiple-writes-to-one-key
  "Returns a checker error if we have multiple writes to one key, or nil if
  things are OK."
  [history]
  (let [res (->> history
                 h/invokes
                 (h/filter (comp write-txn? :value))
                 (reduce (fn [ks op]
                           (let [k (-> op :value first second)]
                             (if (get ks k)
                               (reduced {:valid? :unknown
                                         :error  [:multiple-writes k]})
                               (conj ks k))))
                         #{}))]
    (when (map? res)
      res)))

(defn reads
  "All ok read ops"
  [history]
  (->> history
       h/oks
       (h/filter (comp read-txn? :value))))

(defn early-reads
  "Given a set of read txns finds those that are too early to tell us anything;
  e.g. all nil"
  [reads]
  (->> (map :value reads)
       (remove (partial some mop/value))))

(defn late-reads
  "Given a set of read txns, finds those that are too late to tell us anything;
  e.g. all 1."
  [reads]
  (->> (map :value reads)
       (filter (partial every? mop/value))))

(defn checker
  "Takes a group size n, and a history of :txn transactions. Verifies that no
  key is written multiple times. Searches for read transactions where one read
  observes x but not y, and another observes y but not x."
  [n]
  (reify checker/Checker
    (check [this test history opts]
      (let [reads (reads history)]
        (merge {:reads-count      (count reads)
                :early-read-count (count (early-reads reads))
                :late-read-count  (count (late-reads reads))}
               (or (ensure-no-multiple-writes-to-one-key history)
                   (ensure-no-long-forks n reads)
                   {:valid? true}))))))

(defn workload
  "A package of a checker and generator to look for long forks. n is the group
  size: how many keys to check simultaneously."
  ([] (workload 2))
  ([n]
   {:checker   (checker n)
    :generator (generator n)}))
