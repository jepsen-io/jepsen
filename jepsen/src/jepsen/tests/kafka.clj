(ns jepsen.tests.kafka
  "This workload is intended for systems which behave like the popular Kafka
  queue. This includes Kafka itself, as well as compatible systems like
  Redpanda.

  At the abstract level of this workload, these systems provide a set of
  totally-ordered append-only logs called *partitions*, each of which stores a
  single arbitrary (and, for our purposes, unique) *message* at a particular
  *offset* into the log. Partitions are grouped together into *topics*: each
  topic is therefore partially ordered.

  Each client has a *producer* and a *consumer* aspect; in Kafka these are
  separate clients, but for Jepsen's purposes we combine them. A
  producer can *send* a message to a topic-partition, which assigns it a
  unique, theoretically monotonically-increasing offset and saves it durably at
  that offset. A consumer can *subscribe* to a topic, in which case the system
  aautomatically assigns it any number of partitions in that topic--this
  assignment can change at any time. Consumers can also assign themselves
  specific partitions manually. When a consumer *polls*, it receives messages
  and their offsets from whatever topic-partitions it is currently assigned to,
  and advances its internal state so that the next poll (barring a change in
  assignment) receives the immediately following messages.

  ## Operations

  To subscribe to a new set of topics, we issue an operation like:

    {:f :subscribe, :value [k1, k2, ...]}

  or

    {:f :assign, :value [k1, k2, ...]}

  ... where k1, k2, etc denote specific partitions. For subscribe,
  we convert those partitions to the topics which contain them, and subscribe
  to those topics; the database then controls which specific partitions we get.
  Just like the Kafka client API, both subscribe and assign replace the current
  topics for the consumer.

  Assign ops can also have a special key `:seek-to-beginning? true` which
  indicates that the client should seek to the beginning of all its partitions.

  Reads and writes (and mixes thereof) are encoded as a vector of
  micro-operations:

    {:f :poll, :value [op1, op2, ...]}
    {:f :send, :value [op1, op2, ...]}
    {:f :txn,  :value [op1, op2, ...]}

  Where :poll and :send denote transactions comprising only reads or writes,
  respectively, and :txn indicates a general-purpose transaction. Operations
  are of two forms:

    [:send key value]

  ... instructs a client to append `value` to the integer `key`--which maps
  uniquely to a single topic and partition. These operations are returned as:

    [:send key [offset value]]

  where offset is the returned offset of the write, if available, or `nil` if
  it is unknown (e.g. if the write times out).

  Reads are invoked as:

    [:poll]

  ... which directs the client to perform a single `poll` operation on its
  consumer. The results of that poll are expanded to:

    [:poll {key1 [[offset1 value1] [offset2 value2] ...],
            key2 [...]}]

  Where key1, key2, etc are integer keys obtained from the topic-partitions
  returned by the call to poll, and the value for that key is a vector of
  [offset value] pairs, corresponding to the offset of that message in that
  particular topic-partition, and the value of the message---presumably,
  whatever was written by `[:send key value]` earlier.

  When polling *without* using assign, clients should call `.commitSync` before
  returning a completion operation.

  Before a transaction completes, we commit its offsets.

  All transactions may return an optional key :rebalance-log, which is a vector
  of rebalancing events (changes in assigned partitions) that occurred during
  the execution of that transaction. Each rebalance event is a map like:

    {:keys [k1 k2 ...]}

  There may be more keys in this map; I can't remember right now.

  ## Topic-partition Mapping

  We identify topics and partitions using abstract integer *keys*, rather than
  explicit topics and partitions. The client is responsible for mapping these
  keys bijectively to topics and partitions.

  ## Analysis

  From this history we can perform a number of analyses:

  1. For any observed value of a key, we check to make sure that its writer was
  either :ok or :info; if the writer :failed, we know this constitutes an
  aborted read.

  2. We verify that all sends and polls agree on the value for a given key and
  offset. We do not require contiguity in offsets, because transactions add
  invisible messages which take up an offset slot but are not visible to the
  API. If we find divergence, we know that Kakfa disagreed about the value at
  some offset.

  Having verified that each [key offset] pair uniquely identifies a single
  value, we eliminate the offsets altogether and perform the remainder of the
  analysis purely in terms of keys and values. We construct a graph where
  vertices are values, and an edge v1 -> v2 means that v1 immediately precedes
  v2 in the offset order (ignoring gaps in the offsets, which we assume are due
  to transaction metadata messages).

  3. For each key, we take the highest observed offset, and then check that
  every :ok :send operation with an equal or lower offset was *also* read by at
  least one consumer. If we find one, we know a write was lost!

  4. We build a dependency graph between pairs of transactions T1 and T2, where
  T1 != T2, like so:

    ww. T1 sent value v1 to key k, and T2 sent v2 to k, and o1 < o2
        in the version order for k.

    wr. T1 sent v1 to k, and T2's highest read of k was v1.

    rw. T1's highest read of key k was offset o1, and T2 sent offset o2 to k,
        and o1 < o2 in the version order for k.

  Our use of \"highest offset\" is intended to capture the fact that each poll
  operation observes a *range* of offsets, but in general those offsets could
  have been generated by *many* transactions. If we drew wr edges for every
  offset polled, we'd generate superfluous edges--all writers are already
  related via ww dependencies, so the final wr edge, plus those ww edges,
  captures those earlier read values.

  We draw rw edges only for the final versions of each key observed by a
  transaction. If we drew rw edges for an earlier version, we would incorrectly
  be asserting that later transactions were *not* observed!

  We perform cycle detection and categorization of anomalies from this graph
  using Elle.

  5. Internal Read Contiguity: Within a transaction, each pair of reads on the
  same key should be directly related in the version order. If we observe a gap
  (e.g. v1 < ... < v2) that indicates this transaction skipped over some
  values. If we observe an inversion (e.g. v2 < v1, or v2 < ... < v1) then we
  know that the transaction observed an order which disagreed with the \"true\"
  order of the log.

  6. Internal Write Contiguity: Gaps between sequential pairs of writes to the
  same key are detected via Elle as write cycles. Inversions are not, so we
  check for them explicitly: a transaction sends v1, then v2, but v2 < v1 or v2
  < ... v1 in the version order.

  7. Intermediate reads? I assume these happen constantly, but are they
  supposed to? It's not totally clear what this MEANS, but I think it might
  look like a transaction T1 which writes [v1 v2 v3] to k, and another T2 which
  polls k and observes any of v1, v2, or v3, but not *all* of them. This
  miiight be captured as a wr-rw cycle in some cases, but perhaps not all,
  since we're only generating rw edges for final reads."
  (:require [analemma [xml :as xml]
                      [svg :as svg]]
            [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ real-pmap loopr]]
            [elle [core :as elle]
                  [graph :as g]
                  [list-append :refer [rand-bg-color]]
                  [txn :as txn]
                  [util :refer [index-of]]]
            [gnuplot.core :as gnuplot]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [store :as store]
                    [util :as util :refer [map-vals
                                           meh
                                           nanos->secs
                                           pprint-str]]]
            [jepsen.checker.perf :as perf]
            [jepsen.tests.cycle.append :as append]
            [slingshot.slingshot :refer [try+ throw+]]))

;; Generator

(defn txn-generator
  "Takes a list-append generator and rewrites its transactions to be [:poll] or
  [:send k v] micro-ops. Also adds a :keys field onto each operation, with a
  set of keys that txn would have interacted with; we use this to generate
  :subscribe ops later."
  [la-gen]
  (gen/map (fn rewrite-op [op]
             (-> op
                 (assoc :keys (set (map second (:value op))))
                 (update :value
                         (partial mapv (fn rewrite-mop [[f k v]]
                                         (case f
                                           :append [:send k v]
                                           :r      [:poll]))))))
           la-gen))

(def subscribe-ratio
  "How many subscribe ops should we issue per txn op?"
  1/8)

(defrecord InterleaveSubscribes [gen]
  gen/Generator
  (op [this test context]
    ; When we're asked for an operation, ask the underlying generator for
    ; one...
    (when-let [[op gen'] (gen/op gen test context)]
      (if (= :pending op)
        [:pending this]
        (let [this' (InterleaveSubscribes. gen')]
          (if (< (rand) subscribe-ratio)
            ; At random, emit a subscribe/assign op instead.
            (let [f  (rand-nth (vec (:sub-via test)))
                  op {:f f, :value (vec (:keys op))}]
              [(gen/fill-in-op op context) this])
            ; Or pass through the op directly
            [(dissoc op :keys) (InterleaveSubscribes. gen')])))))

  ; Pass through updates
  (update [this test context event]
    (InterleaveSubscribes. (gen/update gen test context event))))

(defn interleave-subscribes
  "Takes a txn generator and keeps track of the keys flowing through it,
  interspersing occasional :subscribe or :assign operations for recently seen
  keys."
  [txn-gen]
  (InterleaveSubscribes. txn-gen))

(defn tag-rw
  "Takes a generator and tags operations as :f :poll or :send if they're
  entirely comprised of send/polls."
  [gen]
  (gen/map (fn tag-rw [op]
             (case (->> op :value (map first) set)
               #{:poll}  (assoc op :f :poll)
               #{:send}  (assoc op :f :send)
               op))
           gen))

(defn op->max-poll-offsets
  "Takes an operation and returns a map of keys to the highest offsets polled."
  [{:keys [type f value]}]
  (case type
    (:info, :ok)
    (case f
      (:poll, :txn)
      (reduce (fn [offsets [f :as mop]]
                (case f
                  :poll (->> (second mop)
                             (map-vals (fn [pairs]
                                         (->> pairs
                                              (map first)
                                              (remove nil?)
                                              (reduce max -1))))
                             (merge-with max offsets))
                  :send offsets))
              {}
              value)
      nil)
    nil))

(defn op->max-send-offsets
  "Takes an operation and returns a map of keys to the highest offsets sent."
  [{:keys [type f value]}]
  (case type
    (:info, :ok)
    (case f
      (:send, :txn)
      (reduce (fn [offsets [f :as mop]]
                (case f
                  :poll offsets
                  :send (let [[_ k v] mop]
                          (when (and (vector? v) (first v))
                            (assoc offsets k (max (get offsets k 0)
                                                  (first v)))))))
              {}
              value)
      nil)
    nil))

(defn op->max-offsets
  "Takes an operation (presumably, an OK or info one) and returns a map of keys
  to the highest offsets interacted with, either via send or poll, in that op."
  [op]
  (merge-with max
              (op->max-poll-offsets op)
              (op->max-send-offsets op)))

(defrecord PollUnseen [gen sent polled]
  ; sent and polled are both maps of keys to maximum offsets sent and polled,
  ; respectively.
  gen/Generator
  (op [this test context]
    (when-let [[op gen'] (gen/op gen test context)]
      (if (= :pending op)
        [:pending this]
        (let [this' (PollUnseen. gen' sent polled)]
          ; About 1/3 of the time, insert our sent-but-unpolled keys in place of
          ; assign/subscribe
          (if (and (< (rand) 1/3)
                   (or (= :assign    (:f op))
                       (= :subscribe (:f op))))
            [(assoc op :value (->> (:value op)
                                   (concat (keys sent))
                                   distinct
                                   vec))
             this']

            ; Pass through
            [op this'])))))

  ; Look at the offsets we sent and polled, and advance our state to match
  (update [this test context event]
    (if (= :ok (:type event))
      (let [sent'   (merge-with max sent   (op->max-send-offsets event))
            polled' (merge-with max polled (op->max-poll-offsets event))
            ; Trim keys we're caught up on
            [sent' polled']
            (loopr [sent   sent'
                    polled polled']
                   [k (distinct (concat (keys sent') (keys polled')))]
                   (if (< (polled k -1) (sent k -1))
                     ; We have unseen elements
                     (recur sent polled)
                     ; We're caught up!
                     (recur (dissoc sent k) (dissoc polled k))))]
        (PollUnseen. (gen/update gen test context event) sent' polled'))
      ; No change
      this)))

(defn poll-unseen
  "Wraps a generator. Keeps track of every offset that is successfully sent,
  and every offset that's successfully polled. When there's a key that has some
  offsets which were sent but not polled, we consider that unseen. This
  generator occasionally rewrites assign/subscribe operations to try and catch
  up to unseen keys."
  [gen]
  (PollUnseen. gen {} {}))

(defrecord TrackKeyOffsets [gen offsets]
  gen/Generator
  (op [this test context]
    (when-let [[op gen'] (gen/op gen test context)]
      (if (= :pending op)
        [:pending this]
        [op (TrackKeyOffsets. gen' offsets)])))

  (update [this test context event]
    (when (= :ok (:type event))
      (let [op-offsets (op->max-offsets event)]
        (when-not (empty? op-offsets)
          (swap! offsets #(merge-with max % op-offsets)))))
    (TrackKeyOffsets.
      (gen/update gen test context event) offsets)))

(defn track-key-offsets
  "Wraps a generator. Keeps track of every key that generator touches in the
  given atom, which is a map of keys to highest offsets seen."
  [keys-atom gen]
  (TrackKeyOffsets. gen keys-atom))

(defrecord FinalPolls [target-offsets gen]
  gen/Generator
  (op [this test context]
    ;(info "waiting for" target-offsets)
    (when-not (empty? target-offsets)
      (when-let [[op gen'] (gen/op gen test context)]
        [op (assoc this :gen gen')])))

  (update [this test context {:keys [type f value] :as event}]
    ; (info :update event)
    (if (and (= type  :ok)
             (= f     :poll))
      (let [offsets' (reduce (fn [target-offsets' [k seen-offset]]
                               (if (<= (get target-offsets' k -1)
                                       seen-offset)
                                 ; We've read past our target offset for
                                 ; this key
                                 (dissoc target-offsets' k)
                                 target-offsets'))
                             target-offsets
                             (op->max-offsets event))]
        (when-not (identical? target-offsets offsets')
          (info "Process" (:process event) "now waiting for" offsets'))
        (FinalPolls. offsets' gen))
      ; Not relevant
      this)))

(defn final-polls
  "Takes an atom containing a map of keys to offsets. Constructs a generator
  which:

  1. Checks the topic-partition state from the admin API

  2. Crashes the client, to force a fresh one to be opened, just in case
     there's broken state inside the client.

  3. Assigns the new client to poll every key, and seeks to the beginning

  4. Polls repeatedly

  This process repeats every 10 seconds until polls have caught up to the
  offsets in the offsets atom."
  [offsets]
  (delay
    (let [offsets @offsets]
      (info "Polling up to offsets" offsets)
      (->> [{:f :crash}
            {:f :debug-topic-partitions, :value (keys offsets)}
            {:f :assign, :value (keys offsets), :seek-to-beginning? true}
            (->> {:f :poll, :value [[:poll]], :poll-ms 1000}
                 repeat
                 (gen/stagger 1/5))]
           (gen/time-limit 10000)
           repeat
           (FinalPolls. offsets)))))

(defn crash-client-gen
  "A generator which, if the test has :crash-clients? true, periodically emits
  an operation to crash a random client."
  [opts]
  (when (:crash-clients? opts)
    (->> (repeat {:f :crash})
         (gen/stagger (/ (:crash-client-interval opts 30)
                         (:concurrency opts))))))

;; Checker ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn assocv
  "An assoc on vectors which allows you to assoc at arbitrary indexes, growing
  the vector as needed. When v is nil, constructs a fresh vector rather than a
  map."
  [v i value]
  (if v
    (if (<= i (count v))
      (assoc v i value)
      (let [nils (repeat (- i (count v)) nil)]
        (assoc (into v nils) i value)))
    ; Nil is treated as an empty vector.
    (recur [] i value)))

(defn nth+
  "Nth for vectors, but returns nil instead of out-of-bounds."
  [v i]
  (when (< i (count v))
    (nth v i)))

(defn op-writes-helper
  "Takes an operation and a function which takes an offset-value pair. Returns
  a map of keys written by this operation to the sequence of (f [offset value])
  sends for that key. Note that offset may be nil."
  [op f]
  (when (#{:txn :send} (:f op))
    (reduce (fn [writes mop]
              (if (= :send (first mop))
                (let [[_ k v] mop
                      vs (get writes k [])
                      ; Values can be either a literal value or a [offset value]
                      ; pair.
                      value (if (vector? v)
                              (f v)
                              (f [nil v]))]
                  (assoc writes k (conj vs value)))
                ; Something other than a send
                writes))
            {}
            (:value op))))

(defn op-writes
  "Returns a map of keys to the sequence of all values written to that key in
  an op."
  [op]
  (op-writes-helper op second))

(defn op-write-offsets
  "Returns a map of keys to the sequence of all offsets written to that key in
  an op."
  [op]
  (op-writes-helper op first))

(defn op-write-pairs
  "Returns a map of keys to the sequence of all [offset value] pairs written to
  that key in an op."
  [op]
  (op-writes-helper op identity))

(defn op-reads-helper
  "Takes an operation and a function which takes an offset-value pair. Returns
  a map of keys read by this operation to the sequence of (f [offset value])
  read for that key."
  [op f]
  (when (#{:txn :poll} (:f op))
    (reduce (fn mop [res mop]
              (if (= :poll (first mop))
                (reduce (fn per-key [res [k pairs]]
                          (let [vs  (get res k [])
                                vs' (into vs (map f pairs))]
                            (assoc res k vs')))
                        res
                        (second mop))
                res))
            {}
            (:value op))))

(defn op-read-pairs
  "Returns a map of keys to the sequence of all [offset value] pairs read for
  that key."
  [op]
  (op-reads-helper op identity))

(defn op-read-offsets
  "Returns a map of keys to the sequence of all offsets read for that key."
  [op]
  (op-reads-helper op first))

(defn op-reads
  "Returns a map of keys to the sequence of all values read for that key."
  [op]
  (op-reads-helper op second))

(defn op-pairs
  "Returns a map of keys to the sequence of all [offset value] pairs either
  written or read for that key; writes first."
  [op]
  (merge-with concat (op-write-pairs op) (op-read-pairs op)))

(defn reads-of-key
  "Returns a seq of all operations which read the given key, and, optionally,
  read the given value."
  ([k history]
   (->> history
        (filter (comp #{:txn :send :poll} :f))
        (filter (fn [op]
                  (contains? (op-reads op) k)))))
  ([k v history]
   (->> history
        (reads-of-key k)
        (filter (fn [op]
                  (some #{v} (get (op-reads op) k)))))))

(defn writes-of-key
  "Returns a seq of all operations which wrote the given key, and, optionally,
  sent the given value."
  ([k history]
   (->> history
        (filter (comp #{:txn :send :poll} :f))
        (filter (fn [op]
                  (contains? (op-writes op) k)))))
  ([k v history]
   (->> history
        (writes-of-key k)
        (filter (fn [op]
                  (some #{v} (get (op-writes op) k)))))))

(defn reads-of-key-offset
  "Returns a seq of all operations which read the given key and offset."
  [k offset history]
  (->> history
       (reads-of-key k)
       (filter (fn [op]
                 (some #{offset} (get (op-read-offsets op) k))))))

(defn writes-of-key-offset
  "Returns a seq of all operations which wrote the given key and offset."
  [k offset history]
  (->> history
       (writes-of-key k)
       (filter (fn [op]
                 (some #{offset} (get (op-write-offsets op) k))))))

(defn reads-of-key-value
  "Returns a seq of all operations which read the given key and value."
  [k value history]
  (->> history
       (reads-of-key k)
       (filter (fn [op] (some #{value} (get (op-reads op) k))))))

(defn writes-of-key-value
  "Returns a seq of all operations which wrote the given key and value."
  [k value history]
  (->> history
       (writes-of-key k)
       (filter (fn [op] (some #{value} (get (op-writes op) k))))))

(defn op-around-key-offset
  "Takes an operation and returns that operation with its value trimmed so that
  any send/poll operations are constrained to just the given key, and values
  within n of the given offset. Returns nil if operation is not relevant."
  ([k offset op]
   (op-around-key-offset k offset 3 op))
  ([k offset n op]
   (when (and (not= :invoke (:type op))
              (#{:send :poll :txn} (:f op)))
     (let [value'
           (keep (fn [[f v :as mop]]
                   (case f
                     :poll (when-let [pairs (get v k)]
                             ; Trim pairs to region around offset
                             (let [trimmed
                                   (filter (fn [[o v]]
                                             (<= (- offset n) o (+ offset n)))
                                           pairs)]
                               (when (seq trimmed)
                                 [:poll {k trimmed}])))
                     :send (let [[_ k2 v-or-pair] mop]
                             (when (vector? v-or-pair)
                               (let [[o v] v-or-pair]
                                 (when (and (= k k2)
                                            (<= (- offset n) o (+ offset n)))
                                   mop))))))
                 (:value op))]
       (when-not (empty? value')
         (assoc op :value value'))))))

(defn around-key-offset
  "Filters a history to just those operations around a given key and offset;
  trimming their mops to just those regions as well."
  ([k offset history]
   (around-key-offset k offset 3 history))
  ([k offset n history]
   (keep (partial op-around-key-offset k offset n) history)))

(defn around-some
  "Clips a sequence to just those elements near a predicate. Takes a predicate,
  a range n, and a sequence xs. Returns the series of all x in xs such x is
  within n elements of some x' matching predicate."
  [pred n coll]
  (let [indices (first
                  (reduce (fn [[indices i] x]
                            (if (pred x)
                              [(into indices (range (- i n) (+ i n 1))) (inc i)]
                              [indices (inc i)]))
                          [#{} 0]
                          coll))]
    (first (reduce (fn [[out i] x]
                     (if (indices i)
                       [(conj out x) (inc i)]
                       [out (inc i)]))
                   [[] 0]
                   coll))))

(defn op-around-key-value
  "Takes an operation and returns that operation with its value trimmed so that
  any send/poll operations are constrained to just the given key, and values
  within n of the given value. Returns nil if operation is not relevant."
  ([k value op]
   (op-around-key-value k value 3 op))
  ([k value n op]
   (when (and (= :ok (:type op))
              (#{:send :poll :txn} (:f op)))
     (let [value'
           (keep (fn [[f v :as mop]]
                   (case f
                     :poll (when-let [pairs (get v k)]
                             ; Trim pairs to region around offset
                             (let [trimmed (around-some (comp #{value} second)
                                                        n pairs)]
                               (when (seq trimmed)
                                 {k trimmed})))
                     :send (let [[_ k2 [o v]] mop]
                             (when (and (= k k2) (= value v))
                               mop))))
                 (:value op))]
       (when-not (empty? value')
         (assoc op :value value'))))))

(defn around-key-value
  "Filters a history to just those operations around a given key and value;
  trimming their mops to just those regions as well."
  ([k value history]
   (around-key-value k value 3 history))
  ([k value n history]
   (keep (partial op-around-key-value k value n) history)))

(defn writes-by-type
  "Takes a history and constructs a map of types (:ok, :info, :fail) to maps of
  keys to the set of all values which were written for that key. We use this to
  identify, for instance, what all the known-failed writes were for a given
  key."
  [history]
  (->> history
       (remove (comp #{:invoke} :type))
       (filter (comp #{:txn :send} :f))
       (group-by :type)
       (map-vals (fn [ops]
                   (->> ops
                        ; Construct a seq of {key [v1 v2 ...]} maps
                        (map op-writes)
                        ; And turn [v1 v2 ...] into #{v1 v2 ...}
                        (map (partial map-vals set))
                        ; Then merge them all together
                        (reduce (partial merge-with set/union) {}))))))

(defn reads-by-type
  "Takes a history and constructs a map of types (:ok, :info, :fail) to maps of
  keys to the set of all values which were read for that key. We use this to
  identify, for instance, the known-successful reads for some key as a part of
  finding lost updates."
  [history]
  (->> history
       (remove (comp #{:invoke} :type))
       (filter (comp #{:txn :poll} :f))
       (group-by :type)
       (map-vals (fn [ops]
                   (->> ops
                        (map op-reads)
                        (map (partial map-vals set))
                        (reduce (partial merge-with set/union) {}))))))


(defn must-have-committed?
  "Takes a reads-by-type map and a (presumably :info) transaction which sent
  something. Returns true iff the transaction was :ok, or if it was :info and
  we can prove that some send from this transaction was successfully read."
  [reads-by-type op]
  (or (= :ok (:type op))
      (and (= :info (:type op))
           (let [ok (:ok reads-by-type)]
             (some (fn [[k vs]]
                     (let [ok-vs (get ok k #{})]
                       (some ok-vs vs)))
                   (op-writes op))))))

(defn version-orders-update-log
  "Updates a version orders log with the given offset and value."
  [log offset value]
  (if-let [values (nth+ log offset)]
    (assoc  log offset (conj values value)) ; Already have values
    (assocv log offset #{value}))) ; First time we've seen this offset

(defn version-orders-reduce-mop
  "Takes a logs object from version-orders and a micro-op, and integrates that
  micro-op's information about offsets into the logs."
  [logs mop]
  (case (first mop)
    :send (let [[_ k v] mop]
            (if (vector? v)
              (let [[offset value] v]
                (if offset
                  ; We completed the send and know an offset
                  (update logs k version-orders-update-log offset value)
                  ; Not sure what the offset was
                  logs))
              ; Not even offset structure: maybe an :info txn
              logs))

    :poll (reduce (fn poll-key [logs [k pairs]]
                    (reduce (fn pair [logs [offset value]]
                              (if offset
                                (update logs k version-orders-update-log
                                        offset value)
                                logs))
                            logs
                            pairs))
                  logs
                  (second mop))))

(defn index-seq
  "Takes a seq of distinct values, and returns a map of:

    {:by-index    A vector of the sequence
     :by-value    A map of values to their indices in the vector.}"
  [xs]
  {:by-index (vec xs)
   :by-value (into {} (map-indexed (fn [i x] [x i]) xs))})

(defn log->value->first-index
  "Takes a log: a vector of sets of read values for each offset in a partition,
  possibly including `nil`s. Returns a map which takes a value to the index
  where it first appeared."
  [log]
  (->> (remove nil? log)
       (reduce (fn [[earliest i] values]
                 [(reduce (fn [earliest value]
                            (if (contains? earliest value)
                              earliest
                              (assoc earliest value i)))
                          earliest
                          values)
                  (inc i)])
               [{} 0])
       first))

(defn log->last-index->values
  "Takes a log: a vector of sets of read values for each offset in a partition,
  possibly including `nil`s. Returns a vector which takes indices (dense
  offsets) to sets of values whose *last* appearance was at that position."
  [log]
  (->> (remove nil? log)
       ; Build up a map of values to their latest indexes
       (reduce (fn latest [[latest i] values]
                 [(reduce (fn [latest value]
                            (assoc latest value i))
                          latest
                          values)
                  (inc i)])
               [{} 0])
       first
       ; Then invert that map into a vector of indexes to sets
       (reduce (fn [log [value index]]
                 (let [values (get log index #{})]
                   (assocv log index (conj values value))))
               [])))

(defn version-orders
  "Takes a history and a reads-by-type structure. Constructs a map of:

  {:orders   A map of keys to orders for that key. Each order is a map of:
               {:by-index        A vector which maps indices to single values,
                                 in log order.
                :by-value        A map of values to indices in the log.
                :log             A vector which maps offsets to sets of values
                                 in log order.}

   :errors   A series of error maps describing any incompatible orders, where
             a single offset for a key maps to multiple values.}

  Offsets are directly from Kafka. Indices are *dense* offsets, removing gaps
  in the log."
  ([history reads-by-type]
   (version-orders history reads-by-type {}))
  ([history reads-by-type logs]
   ; Logs is a map of keys to vectors, where index i in one of those vectors is
   ; the vector of all observed values for that index.
   (if (seq history)
     (let [op       (first history)
           history' (next history)]
       (if (must-have-committed? reads-by-type op)
         (case (:f op)
           (:poll, :send, :txn)
           (recur history' reads-by-type
                  (reduce version-orders-reduce-mop logs (:value op)))
           ; Some non-transactional op, like an assign/subscribe
           (recur history' reads-by-type logs))
         ; We can't assume this committed; don't consider it.
         (recur history' reads-by-type logs)))
     ; All done; transform our logs to orders.
     {:errors (->> logs
                   (mapcat (fn errors [[k log]]
                             (->> log
                                  (reduce (fn [[offset index errs] values]
                                            (condp <= (count values)
                                              ; Divergence
                                              2 [(inc offset) (inc index)
                                                 (conj errs {:key    k
                                                             :offset offset
                                                             :index  index
                                                             :values values})]
                                              ; No divergence
                                              1 [(inc offset) (inc index) errs]
                                              ; Hole in log
                                              0 [(inc offset) index errs]))
                                          [0 0 []])
                                  last)))
                   seq)
      :orders
      (map-vals
        (fn key-order [log]
          (assoc (->> log (remove nil?) (map first) index-seq)
                 :log log))
        logs)})))

(defn g1a-cases
  "Takes a partial analysis and looks for aborted reads, where a known-failed
  write is nonetheless visible to a committed read. Returns a seq of error
  maps, or nil if none are found."
  [{:keys [history writes-by-type writer-of]}]
  (let [failed (:fail writes-by-type)
        ops (->> history
                 (filter (comp #{:ok} :type))
                 (filter (comp #{:txn :poll} :f)))]
    (->> (for [op     ops
               [k vs] (op-reads op)
               v      vs
               :when (contains? (get failed k) v)]
           {:key    k
            :value  v
            :writer (get-in writer-of [k v])
            :reader op})
         seq)))

(defn lost-write-cases
  "Takes a partial analysis and looks for cases of lost write: where a write
  that we *should* have observed is somehow not observed. Of course we cannot
  expect to observe everything: for example, if we send a message to Redpanda
  at the end of a test, and don't poll for it, there's no chance of us seeing
  it at all! Or a poller could fall behind.

  What we do instead is identify the highest read value for each key v_max, and
  then take the set of all values *prior* to it in the version order: surely,
  if we read v_max = 3, and the version order is [1 2 3 4], we should also have
  read 1 and 2.

  It's not *quite* this simple. If a message appears at multiple offsets, the
  version order will simply pick one for us, which leads to nondeterminism. If
  an offset has multiple messages, a successfully inserted message could appear
  *nowhere* in the version order.

  To deal with this, we examine the raw logs for each key, and build two index
  structures. The first maps values to their earliest (index) appearance in the
  log: we use this to determine the highest index that must have been read. The
  second is a vector which maps indexes to sets of values whose *last*
  appearance in the log was at that index. We use this vector to identify which
  values ought to have been read.

  Once we've derived the set of values we ought to have read for some key k, we
  run through each poll of k and cross off the values read. If there are any
  values left, they must be lost updates."
  [{:keys [history version-orders reads-by-type writer-of readers-of]}]
  ; Start with all the values we know were read
  (->> (:ok reads-by-type)
       (mapcat
         (fn [[k vs]]
           ; Great, now for this key, find the highest index observed
           (let [vo         (get version-orders k)
                 ; For each value, what's the earliest index we observed
                 ; that value at?
                 value->first-index (log->value->first-index (:log vo))
                 ; And for each index, which values appeared at that index
                 ; *last*?
                 last-index->values (log->last-index->values (:log vo))

                 ; From each value, we take the latest of the earliest
                 ; indices that value appeared at.
                 bound (->> vs
                            ; We might observe a value but *not* know
                            ; its offset from either write or read.
                            ; When this happens, we can't say anything
                            ; about how much of the partition should
                            ; have been observed, so we skip it.
                            (keep value->first-index)
                            (reduce max -1))
                 ; Now take a prefix of last-index->values up to that
                 ; index; these are the values we should have observed.
                 must-read (->> (inc bound)
                                (subvec last-index->values 0)
                                ; This is a vector of sets. We glue them
                                ; together this way to preserve index order.
                                (mapcat identity)
                                distinct)
                 ; To demonstrate these errors to users, we want to prove that
                 ; some reader observed the maximum offset (and therefore
                 ; someone else should have observed lower offsets).
                 max-read (->> (nth last-index->values bound)
                               first ; If there were conflicts, any value ok
                               (vector k)
                               (get-in readers-of)
                               first)
                 ; Now we go *back* to the read values vs, and strip them
                 ; out from the must-read set; anything left is something we
                 ; failed to read.
                 lost (remove vs must-read)
                 ; Because we performed this computation based on the
                 ; version order, we may have flagged info/fail writes as
                 ; lost. We need to go through and check that the writers
                 ; are either a.) OK, or b.) info AND one of their writes
                 ; was read.
                 lost
                 (filter
                   (fn double-check [v]
                     (let [w (or (get-in writer-of [k v])
                                 (throw+ {:type  :no-writer-of
                                          :key   k
                                          :value v}))]
                       (must-have-committed? reads-by-type w)))
                   lost)]
             (->> lost
                  (map (fn [v]
                         {:key            k
                          :value          v
                          :index          (get value->first-index v)
                          :max-read-index bound
                          :writer         (get-in writer-of [k v])
                          :max-read       max-read}))))))
       seq))

(defn strip-types
  "Takes a collection of maps, and removes their :type fields. Returns nil if
  none remain."
  [ms]
  (seq (map (fn [m] (dissoc m :type)) ms)))

(defn int-poll-skip+nonmonotonic-cases
  "Takes a partial analysis and looks for cases where a single transaction
  contains:

    {:skip          A pair of poll values which read the same key and skip
                    over some part of the log which we know should exist.
     :nonmonotonic A pair of poll values which *contradict* the log order,
                   or repeat the same value.}

  When a transaction's rebalance log includes a key which would otherwise be
  involved in one of these violations, we don't report it as an error: we
  assume that rebalances invalidate any assumption of monotonically advancing
  offsets."
  [{:keys [history version-orders]}]
  (->> history
       (pmap
         (fn per-op [op]
           (let [rebalanced-keys (->> op :rebalance-log
                                      (mapcat :keys)
                                      set)]
             ; Consider each pair of reads of some key in this op...
             (->> (for [[k vs]  (op-reads op)
                        [v1 v2] (partition 2 1 vs)]
                    (let [{:keys [by-index by-value]} (get version-orders k)
                          ; What are their indices in the log?
                          i1 (get by-value v1)
                          i2 (get by-value v2)
                          delta (if (and i1 i2)
                                  (- i2 i1)
                                  1)]
                      (cond ; If a key was rebalanced during the transaction,
                            ; all bets are off.
                            (rebalanced-keys k)
                            nil

                            (< 1 delta)
                            {:type     :skip
                             :key      k
                             :values   [v1 v2]
                             :delta    delta
                             :skipped  (map by-index (range (inc i1) i2))
                             :op       op}

                            (< delta 1)
                            {:type    :nonmonotonic
                             :key     k
                             :values  [v1 v2]
                             :delta   delta
                             :op      op})))
                  (remove nil?)))))
       (mapcat identity)
       (group-by :type)
       (map-vals strip-types)))

(defn int-send-skip+nonmonotonic-cases
  "Takes a partial analysis and looks for cases where a single transaction
  contains a pair of sends to the same key which:

    {:skip          Skips over some indexes of the log
     :nonmonotonic  Go backwards (or stay in the same place) in the log}"
  [{:keys [history version-orders]}]
  (->> history
       (remove (comp #{:invoke} :type))
       (mapcat (fn per-op [op]
                 ; Consider each pair of sends to a given key in this op...
                 (->> (for [[k vs]  (op-writes op)
                            [v1 v2] (partition 2 1 vs)]
                        (let [{:keys [by-index by-value]} (get version-orders k)
                              i1 (get by-value v1)
                              i2 (get by-value v2)
                              delta (if (and i1 i2)
                                      (- i2 i1)
                                      1)]
                          (cond (< 1 delta)
                                {:type    :skip
                                 :key     k
                                 :values  [v1 v2]
                                 :delta   delta
                                 :skipped (map by-index (range (inc i1) i2))
                                 :op      op}

                                (< delta 1)
                                {:type   :nonmonotonic
                                 :key    k
                                 :values [v1 v2]
                                 :delta  delta
                                 :op     op})))
                      (remove nil?))))
       (group-by :type)
       (map-vals strip-types)))

(defn poll-skip+nonmonotonic-cases
  "Takes a partial analysis and checks each process's operations sequentially,
  looking for cases where a single process either jumped backwards or skipped
  over some region of a topic-partition. Returns a map:

    {:nonmonotonic  Cases where a process started polling at or before a
                    previous operation last left off
     :skip          Cases where two successive operations by a single process
                    skipped over one or more values for some key.}"
  [{:keys [history version-orders]}]
  ; First, group ops by process
  (->> (group-by :process history)
       ; Then reduce each process, keeping track of the most recent read
       ; index for each key.
       (map-vals
         (fn per-process [ops]
           ; Iterate over this process's operations, building up a vector of
           ; errors.
           (loop [ops        ops
                  errors     []
                  last-reads {}]
             (if-not (seq ops)
               ; Done
               errors
               ; Process this op
               (let [op    (first ops)]
                 (case (:f op)
                   ; When we assign or subscribe a new topic, preserve *only*
                   ; those last-reads which we're still subscribing to.
                   (:assign, :subscribe)
                   (recur (next ops)
                          errors
                          (if (#{:invoke :fail} (:type op))
                            last-reads
                            (select-keys last-reads (:value op))))

                   (:txn, :poll)
                   (let [reads (op-reads op)
                         ; Look at each key and values read by this op.
                         errs (for [[k reads] reads]
                                ; What was the last op that read this key?
                                (if-let [last-op (get last-reads k)]
                                  ; Compare the index of the last thing read by
                                  ; the last op read to the index of our first
                                  ; read
                                  (let [vo  (get-in version-orders
                                                    [k :by-value])
                                        v   (-> last-op op-reads (get k) last)
                                        v'  (first reads)
                                        i   (get vo v)
                                        i'  (get vo v')
                                        delta (if (and i i')
                                                (- i' i)
                                                1)]
                                    [(when (< 1 delta)
                                       ; We can show that this op skipped an
                                       ; index!
                                       (let [voi (-> version-orders (get k)
                                                     :by-index)
                                             skipped (map voi
                                                          (range (inc i) i'))]
                                         {:type    :skip
                                          :key     k
                                          :delta   delta
                                          :skipped skipped
                                          :ops     [last-op op]}))
                                     (when (< delta 1)
                                       ; Aha, this wasn't monotonic!
                                       {:type   :nonmonotonic
                                        :key    k
                                        :values [v v']
                                        :delta  delta
                                        :ops    [last-op op]})])
                                  ; First read of this key
                                  nil))
                         errs (->> errs
                                   (mapcat identity)
                                   (remove nil?))
                         ; Update our last-reads index for this op's read keys
                         last-reads' (->> (keys reads)
                                          (reduce (fn update-last-read [lr k]
                                                    (assoc lr k op))
                                                  last-reads))]
                     (recur (next ops) (into errors errs) last-reads'))

                   ; Some other :f
                   (recur (next ops) errors last-reads)))))))
           ; Join together errors from all processes
           (mapcat val)
           (group-by :type)
       (map-vals strip-types)))

(defn nonmonotonic-send-cases
  "Takes a partial analysis and checks each process's operations sequentially,
  looking for cases where a single process's sends to a given key go backwards
  relative to the version order."
  [{:keys [history version-orders]}]
  ; First, consider each process' completions...
  (->> (filter (comp #{:ok :info} :type) history)
       (group-by :process)
       ; Then reduce each process, keeping track of the most recent sent
       ; index for each key.
       (map-vals
         (fn per-process [ops]
           ; Iterate over this process's operations, building up a vector of
           ; errors.
           (loop [ops        ops
                  errors     []
                  last-sends {}] ; Key -> Most recent op to send to that key
             (if-not (seq ops)
               ; Done
               errors
               ; Process this op
               (let [op (first ops)]
                 (case (:f op)
                   ; When we assign or subscribe a new topic, preserve *only*
                   ; those last-reads which we're still subscribing to.
                   (:assign, :subscribe)
                   (recur (next ops)
                          errors
                          (if (#{:invoke :fail} (:type op))
                            last-sends
                            (select-keys last-sends (:value op))))

                   (:send, :txn)
                   (let [; Look at each key and values sent by this op.
                         sends (op-writes op)
                         errs (for [[k sends] sends]
                                ; What was the last op that sent to this key?
                                (if-let [last-op (get last-sends k)]
                                  ; Compare the index of the last thing sent by
                                  ; the last op to the index of our first send
                                  (let [vo  (get-in version-orders
                                                    [k :by-value])
                                        v   (-> last-op op-writes (get k) last)
                                        v'  (first sends)
                                        i   (get vo v)
                                        i'  (get vo v')
                                        delta (if (and i i')
                                                (- i' i)
                                                1)]
                                    (when (< delta 1)
                                      ; Aha, this wasn't monotonic!
                                      {:key    k
                                       :values [v v']
                                       :delta  (- i' i)
                                       :ops    [last-op op]}))
                                  ; First send to this key
                                  nil))
                         errs (->> errs
                                   (remove nil?))
                         ; Update our last-reads index for this op's sent keys
                         last-sends' (->> (keys sends)
                                          (reduce (fn update-last-send [lr k]
                                                    (assoc lr k op))
                                                  last-sends))]
                     (recur (next ops) (into errors errs) last-sends'))

                   ; Some other :f
                   (recur (next ops) errors last-sends)))))))
       ; Join together errors from all processes
       (mapcat val)
       seq))

(defn duplicate-cases
  "Takes a partial analysis and identifies cases where a single value appears
  at more than one offset in a key."
  [{:keys [version-orders]}]
  (->> version-orders
       (mapcat (fn per-key [[k version-order]]
                 (->> (:by-index version-order)
                      frequencies
                      (filter (fn dup? [[value number]]
                                (< 1 number)))
                      (map (fn [[value number]]
                             {:key    k
                              :value  value
                              :count  number})))))
       seq))

(defn unseen
  "Takes a history and yields a series of maps like

    {:time    The time in nanoseconds
     :unseen  A map of keys to the number of messages in that key which have
              been successfully acknowledged, but not polled by any client.}

  The final map in the series includes a :messages key: a map of keys to sets
  of messages that were unseen."
  [history]
  (let [[out unseen]
        (reduce
          ; Out is a vector of output observations. sent is a map of keys to
          ; successfully sent values; polled is a map of keys to successfully
          ; read values.
          (fn red [[out sent polled :as acc] {:keys [type time f] :as op}]
            (if-not (and (= type :ok)
                         (#{:poll :send :txn} f))
              acc
              (let [sent'   (->> op op-writes (map-vals set)
                                 (merge-with set/union sent))
                    polled' (->> op op-reads (map-vals set)
                                 (merge-with set/union polled))
                    unseen  (merge-with set/difference sent' polled')]
                ; We don't have to keep looking for things we've seen, so we can
                ; recur with unseen rather than sent'.
                [(conj! out {:time time, :unseen (map-vals count unseen)})
                 unseen
                 polled'])))
          [(transient []) {} {}]
          history)
        out (persistent! out)]
    (when (seq out)
      (update out (dec (count out))
              assoc :messages unseen))))

(defn plot-unseen!
  "Takes a test, a collection of unseen measurements, and options (e.g. those
  to checker/check). Plots a graph file (unseen.png) in the store directory."
  [test unseen {:keys [subdirectory]}]
  (let [ks       (-> unseen peek :unseen keys sort reverse vec)
        ; Turn these into stacked graphs
        datasets (reduce
                   (fn [datasets {:keys [time unseen]}]
                     ; We go through keys in reverse order so that the last
                     ; key in the legend is on the bottom of the chart
                     (->> ks reverse
                          (reduce (fn [[datasets sum] k]
                                    (if-let [count (get unseen k 0)]
                                      (let [t        (nanos->secs time)
                                            sum'     (+ sum count)
                                            dataset  (get datasets k [])
                                            dataset' (conj dataset [t sum'])]
                                        [(assoc datasets k dataset') sum'])
                                      ; Key doesn't exist yet
                                      [datasets sum]))
                                  [datasets 0])
                          first))
                   {}
                   unseen)
        ; Grab just the final poll section of history
        final-polls (->> test :history rseq
                         (take-while (comp #{:poll :crash :assign
                                             :debug-topic-partitions} :f)))
        ; Draw a line for the start and end of final polls.
        final-poll-lines (->> [(first final-polls) (last final-polls)]
                              (map (comp nanos->secs :time))
                              (map (fn [t]
                                     [:set :arrow
                                      :from (gnuplot/list t [:graph 0])
                                      :to   (gnuplot/list t [:graph 1])
                                      :lc   :rgb "#F3974A"
                                      :lw   "1"
                                      :nohead])))
        output   (.getCanonicalPath
                   (store/path! test subdirectory "unseen.png"))
        preamble (concat (perf/preamble output)
                         [[:set :title (str (:name test) " unseen")]
                          '[set ylabel "Unseen messages"]]
                         final-poll-lines)
        series (for [k ks]
                 {:title (str "key " k)
                  :with '[filledcurves x1]
                  :data (datasets k)})]
    (-> {:preamble preamble, :series series}
        perf/with-range
        perf/plot!
        (try+ (catch [:type :jepsen.checker.perf/no-points] _ :no-points)))))

(defn realtime-lag
  "Takes a history and yields a series of maps of the form

    {:process The process performing a poll
     :key     The key being polled
     :time    The time the read began, in nanos
     :lag     The realtime lag of this key, in nanos.

  The lag of a key k in a poll is the conservative estimate of how long it has
  been since the highest value in that poll was the final message in log k.

  For instance, given:

    {:time 1, :type :ok, :value [:send :x [0 :a]]}
    {:time 2, :type :ok, :value [:poll {:x [0 :a]}]}

  The lag of this poll is zero, since we observed the most recent completed
  write to x. However, if we:

    {:time 3, :type :ok,      :value [:send :x [1 :b]]}
    {:time 4, :type :invoke,  :value [:poll]}
    {:time 5, :type :ok,      :value [:poll {:x []}]}

  The lag of this read is 4 - 3 = 1. By time 3, offset 1 must have existed for
  key x. However, the most recent offset we observed was 0, which could only
  have been the most recent offset up until the write of offset 1 at time 3.
  Since our read could have occurred as early as time 4, the lag is at least 1.

  Might want to make this into actual [lower upper] ranges, rather than just
  the lower bound on lag, but conservative feels OK for starters."
  [history]
  ; First, build up a map of keys to vectors, where element i of the vector for
  ; key k is the time at which we knew offset i+1 was no longer the most recent
  ; thing in the log. Thus the first entry in each vector is the time at which
  ; the log was no longer *empty*. The second entry is the time at which offset
  ; 0 was no longer current, and so on.
  (let [expired
        (reduce
          (fn expired [expired {:keys [type f time value] :as op}]
            ; What are the most recent offsets we know exist?
            (let [known-offsets (op->max-offsets op)]
              (reduce
                (fn [expired [k known-offset]]
                  ; For this particulary key, we know this offset exists.
                  ; Therefore we *also* know every lower offset should
                  ; exist. We zip backwards, filling in offsets with the
                  ; current time, until we hit an offset where a lower
                  ; time exists.
                  (loop [expired-k (get expired k [])
                         i         known-offset]
                    (if (or (neg? i) (get expired-k i))
                      ; We've gone back before the start of the vector, or
                      ; we hit an index that's already known to be expired
                      ; earlier.
                      (assoc expired k expired-k)
                      ; Update this index
                      (recur (assocv expired-k i time) (dec i)))))
                expired
                known-offsets)))
          {}
          history)]
    ; Now work through the history, turning each poll operation into a lag map.
    (loop [history' history
           ; A map of processes to keys to the highest offset that process has
           ; seen. Keys in this map correspond to the keys this process
           ; currently has :assign'ed. If using `subscribe`, keys are filled in
           ; only when they appear in poll operations. The default offset is
           ; -1.
           process-offsets {}
           ; A vector of output lag maps.
           lags []]
      (if-not (seq history')
        lags
        (let [[op & history'] history'
              {:keys [type process f value]} op]
          (if-not (= :ok type)
            ; Only OK ops matter
            (recur history' process-offsets lags)

            (case f
              ; When we assign something, we replace the process's offsets map.
              :assign
              (recur history'
                     (let [offsets (get process-offsets process {})]
                       ; Preserve keys that we're still subscribing to;
                       ; anything else gets reset to offset -1. This is gonna
                       ; break if we use something other than auto_offset_reset
                       ; = earliest, but... deal with that later. I've got
                       ; limited time here and just need to get SOMETHING
                       ; working.
                       (assoc process-offsets process
                              (merge (zipmap value (repeat -1))
                                     (select-keys offsets value))))
                     lags)

              ; When we subscribe, we're not necessarily supposed to get
              ; updates for the key we subscribed to--we subscribe to *topics*,
              ; not individual partitions. We might get *no* updates, if other
              ; subscribers have already claimed all the partitions for that
              ; topic. We reset the offsets map to empty, to be conservative.
              :subscribe
              (recur history' (assoc process-offsets process {}) lags)

              (:poll, :txn)
              (let [invoke-time (:time (h/invocation history op))
                    ; For poll ops, we merge all poll mop results into this
                    ; process's offsets, then figure out how far behind each
                    ; key based on the time that key offset expired.
                    offsets' (merge-with max
                                         (get process-offsets process {})
                                         (op->max-offsets op))
                    lags' (->> offsets'
                               (map (fn [[k offset]]
                                      ; If we've read *nothing*, then we're at
                                      ; offset -1. Element 0 in the expired
                                      ; vector for k tells us the time when the
                                      ; first element was definitely present.
                                      ; If we read offset 0, we want to consult
                                      ; element 1 of the vector, which tells us
                                      ; when offset 0 was no longer the tail,
                                      ; and so on.
                                      (let [expired-k (get expired k [])
                                            i (inc offset)
                                            expired-at (get-in expired
                                                               [k (inc offset)]
                                                               ##Inf)
                                            lag (-> invoke-time
                                                    (- expired-at)
                                                    (max 0))]
                                        {:time    invoke-time
                                         :process process
                                         :key     k
                                         :lag     lag})))
                               (into lags))]
                (recur history'
                       (assoc process-offsets process offsets')
                       lags'))

              ; Anything else, pass through
              (recur history' process-offsets lags))))))))

(defn op->thread
  "Returns the thread which executed a given operation."
  [test op]
  (-> op :process (mod (:concurrency test))))

(defn plot-realtime-lag!
  "Takes a test, a collection of realtime lag measurements, and options (e.g.
  those to checker/check). Plots a graph file (realtime-lag.png) in the store
  directory"
  [test lags {:keys [nemeses subdirectory filename
                     group-fn group-name]}]
  (let [nemeses  (or nemeses (:nemeses (:plot test)))
        datasets (->> lags
                      (group-by group-fn)
                      (map-vals (fn [lags]
                                  ; At any point in time, we want the maximum
                                  ; lag for this thread across any key.
                                  (->> lags
                                       (partition-by :time)
                                       (map (partial util/max-by :lag))
                                       (map (juxt (comp nanos->secs :time)
                                                  (comp nanos->secs :lag)))))))
        output  (.getCanonicalPath
                  (store/path! test subdirectory
                               (or filename "realtime-lag.png")))
        preamble (concat (perf/preamble output)
                         [[:set :title (str (:name test)
                                            " realtime lag by "
                                            group-name)]
                          '[set ylabel "Lag (s)"]])
        series (for [g (util/polysort (keys datasets))]
                 {:title (str group-name " " g)
                  :with 'linespoints
                  :data (datasets g)})]
    (-> {:preamble preamble
         :series series}
        perf/with-range
        perf/plot!
        (try+ (catch [:type :jepsen.checker.perf/no-points] _ :no-points)))))

(defn plot-realtime-lags!
  "Constructs realtime lag plots for all processes together, and then another
  broken out by process, and also by key."
  [test lags opts]
  (->> [{:group-name "thread",     :group-fn (partial op->thread test)}
        {:group-name "key",        :group-fn :key}
        {:group-name "thread-key", :group-fn (juxt (partial op->thread test)
                                                   :key)}]
       (mapv (fn [{:keys [group-name group-fn] :as opts2}]
               (let [opts (merge opts opts2)]
                 (plot-realtime-lag!
                   test lags (assoc opts :filename (str "realtime-lag-"
                                                        group-name ".png")))
                 (->> lags
                      (group-by group-fn)
                      (pmap (fn [[thread lags]]
                              (plot-realtime-lag!
                                test lags
                                (assoc opts
                                       :subdirectory (str "realtime-lag-"
                                                          group-name)
                                       :filename (str thread ".png")))))
                      dorun))))))

(defn worst-realtime-lag
  "Takes a seq of realtime lag measurements, and finds the point with the
  highest lag."
  [lags]
  (or (util/max-by :lag lags) {:time 0 :lag 0}))

(defn key-order-viz
  "Takes a key, a log for that key (a vector of offsets to sets of elements
  which were observed at that offset) and a history of ops relevant to that
  key. Constructs an XML structure visualizing all sends/polls of that log's
  offsets."
  [k log history]
  (let [; Turn an index into a y-coordinate
        i->y      (fn [i] (* i 14))
        ; Turn an offset into an x-coordinate
        offset->x (fn [offset] (* offset 24))
        ; Turn a log, index, and op, and pairs in that op into an SVG element
        row (fn [i {:keys [type time f process value] :as op} pairs]
              (let [y (i->y i)]
                (into [:g [:title (str (name type) " " (name f)
                                       " by process " process "\n"
                                       (pr-str value))]]
                      (for [[offset value] pairs :when offset]
                        (let [compat? (-> log (nth offset) count (< 2))
                              style   (str (when-not compat?
                                             (str "background: "
                                                  (rand-bg-color value) ";")))]
                          [:text (cond-> {:x (offset->x offset)
                                          :y y
                                          :style style})
                           (str value)])))))
        ; Compute rows and bounds
        [_ max-x max-y rows]
        (loopr [i     0
                max-x 0
                max-y 0
                rows  []]
               [op history]
               (if-let [pairs (-> op op-pairs (get k))]
                 (let [row (row i op pairs)]
                   (info :row row)
                   (if-let [cells (-> row next next)]
                     (do (info :cells cells)
                         (let [max-y (->> cells first second :y long
                                          (max max-y))
                               max-x (->> cells
                                          (map (comp :x second))
                                          (reduce max max-x)
                                          long)]
                           (recur (inc i)
                                  max-x
                                  max-y
                                  (conj rows row))))
                     ; No cells here
                     (recur (inc i) max-x max-y (conj rows row))))
                 ; Nothing relevant here, skip it
                 (recur i max-x max-y rows)))
        svg (svg/svg {"version" "2.0"
                      "width"   (+ max-x 20)
                      "height"  (+ max-y 20)}
                     [:style "svg {
                              font-family: Helvetica, Arial, sans-serif;
                              font-size: 10px;
                             }"]
                     (cons :g rows))]
    svg))

(defn render-order-viz!
  "Takes a test, an analysis, and for each key with certain errors
  renders an HTML timeline of how each operation perceived that key's log."
  [test {:keys [version-orders errors history] :as analysis}]
  (let [history (filter (comp #{:ok} :type) history)]
    (->> (select-keys errors [:inconsistent-offsets :duplicate :lost-write])
         (mapcat val)
         (map :key)
         (concat (->> errors :unseen :unseen keys))
         distinct
         (pmap (fn [k]
                 (let [svg (key-order-viz k
                                          (get-in version-orders [k :log])
                                          history)
                       path (store/path! test "orders"
                                         (if (integer? k)
                                           (format "%03d.svg" k)
                                           (str k ".svg")))]
                   (spit path (xml/emit svg)))))
         dorun)))

(defn consume-counts
  "Kafka transactions are supposed to offer 'exactly once' processing: a
  transaction using the subscribe workflow should be able to consume an offset
  and send something to an output queue, and if this transaction is successful,
  it should happen at most once. It's not exactly clear to me *how* these
  semantics are supposed to work--it's clearly not once per consumer group,
  because we routinely see dups with only one consumer group. As a fallback, we
  look for single consumer per process, which should DEFINITELY hold, but...
  appears not to.

  We verify this property by looking at all committed transactions which
  performed a poll while subscribed (not assigned!) and keeping track of the
  number of times each key and value is polled. Yields a map of keys to values
  to consumed counts, wherever that count is more than one."
  [history]
  (loopr [counts      {}  ; process->k->v->count
          subscribed #{}] ; set of processes which are subscribed
         [{:keys [type f process value] :as op} history]
         (if (not= type :ok)
           (recur counts subscribed)
           (case f
             :subscribe (recur counts (conj subscribed process))
             (:txn, :poll)
             (if (subscribed process)
               ; Increment the count for each value read by this txn
               (recur (loopr [counts counts]
                             [[k vs] (op-reads op)
                              v      vs]
                             (recur (update-in
                                      counts [process k v] (fnil inc 0))))
                      subscribed)
               ; Don't care; this might be an assign poll, and assigns are free
               ; to double-consume
               (recur counts subscribed))

             ; Default
             (recur counts subscribed)))
         ; Finally, compute a distribution, and filter out anything which was
         ; only read once.
         (loopr [dist (sorted-map)
                 dups (sorted-map)]
                [[p k->v->count] counts
                 [k v->count]    k->v->count
                 [v count]       v->count]
                (recur (update dist count (fnil inc 0))
                       (if (< 1 count)
                         (let [k-dups' (-> (get dups k (sorted-map))
                                           (assoc v count))]
                           (assoc dups k k-dups'))
                         dups))
                {:distribution dist
                 :dup-counts dups})))

(defn writer-of
  "Takes a history and builds a map of keys to values to the completion
  operation which attempted to write that value."
  [history]
  (loopr [writer-of (transient {})]
         [op     (remove (comp #{:invoke} :type) history)
          [k vs] (op-writes op)
          v      vs]
         (let [k-writer-of  (get writer-of k (transient {}))
               k-writer-of' (assoc! k-writer-of v op)]
           (recur (assoc! writer-of k k-writer-of')))
         (map-vals persistent! (persistent! writer-of))))

(defn readers-of
  "Takes a history and builds a map of keys to values to vectors of completion
  operations which observed those that value."
  [history]
  (loopr [readers (transient {})]
         [op     (remove (comp #{:invoke} :type) history)
          [k vs] (op-reads op)
          v      vs]
         (let [k-readers   (get readers   k (transient {}))
               kv-readers  (get k-readers v [])
               kv-readers' (conj kv-readers op)
               k-readers'  (assoc! k-readers v kv-readers')]
           (recur (assoc! readers k k-readers')))
         (map-vals persistent! (persistent! readers))))

(defn previous-value
  "Takes a version order for a key and a value. Returns the previous value in
  the version order, or nil if either we don't know v2's index or v2 was the
  first value in the version order."
  [version-order v2]
  (when-let [i2 (-> version-order :by-value (get v2))]
    (when (< 0 i2)
      (-> version-order :by-index (nth (dec i2))))))

(defn mop-index
  "Takes an operation, a function f (:poll or :send), a key k, and a value v.
  Returns the index (0, 1, ...) within that operation's value which performed
  that poll or send, or nil if none could be found."
  [op f k v]
  (loopr [i         0
          mop-index nil]
         [[fun a b] (:value op)]
         (if mop-index
           (recur i mop-index)
           (if (and (= f fun)
                    (case f
                      :send (and (= k a)
                                 (if (vector? b)
                                   (= v (second b))
                                   (= v b)))
                      :poll (when-let [pairs (get a k)]
                              (some (comp #{v} second) pairs))))
             (recur (inc i) i)
             (recur (inc i) mop-index)))
         mop-index))

(defrecord WWExplainer [writer-of version-orders]
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (->> (for [[k vs] (op-writes b)
               v2 vs]
           (when-let [v1 (previous-value (version-orders k) v2)]
             (if-let [writer (-> writer-of (get k) (get v1))]
               (when (= a writer)
                 {:type   :ww
                  :key    k
                  :value  v1
                  :value' v2
                  :a-mop-index (mop-index a :send k v1)
                  :b-mop-index (mop-index b :send k v2)})
               (throw+ {:type :no-writer-of-value, :key k, :value v1}))))
         (remove nil?)
         first))

  (render-explanation [_ {:keys [key value value'] :as m} a-name b-name]
    (str a-name " sent " (pr-str value) " to " (pr-str key)
         " before " b-name " sent " (pr-str value'))))

; A trivial explainer which refuses to acknowledge any connection between
; things.
(defrecord NeverExplainer []
  elle/DataExplainer
  (explain-pair-data [_ a b] nil)
  (render-explanation [_ _ _ _] nil))

(defn ww-graph
  "Analyzes a history to extract write-write dependencies. T1 < T2 iff T1 sends
  some v1 to k and T2 sends some v2 to k and v1 < v2 in the version order."
  [{:keys [writer-of version-orders ww-deps]} history]
  {:graph (if-not ww-deps
            ; We might ask not to infer ww dependencies, in which case this
            ; graph is empty.
            (g/named-graph :ww)
            (loopr [g (g/linear (g/digraph))]
                   [[k v->writer] writer-of ; For every key
                    [v2 op2] v->writer]     ; And very value written in that key
                   (let [version-order (get version-orders k)]
                     (if-let [v1 (previous-value version-order v2)]
                       (if-let [op1 (v->writer v1)]
                         (if (= op1 op2)
                           (recur g) ; No self-edges
                           (recur (g/link g op1 op2)))
                         (throw+ {:type   :no-writer-of-value
                                  :key    k
                                  :value  v1}))
                       ; This is the first value in the version order.
                       (recur g)))
                   (g/named-graph :ww (g/forked g))))
   :explainer (if-not ww-deps
                (NeverExplainer.)
                (WWExplainer. writer-of version-orders))})

(defrecord WRExplainer [writer-of]
  elle/DataExplainer
  (explain-pair-data [_ a b]
    (->> (for [[k vs] (op-reads b)
               v vs]
           (if-let [writer (-> writer-of (get k) (get v))]
             (when (= a writer)
               {:type  :wr
                :key   k
                :value v
                :a-mop-index (mop-index a :send k v)
                :b-mop-index (mop-index b :poll k v)})
             (throw+ {:type :no-writer-of-value, :key k, :value v})))
         (remove nil?)
         first))

  (render-explanation [_ {:keys [key value] :as m} a-name b-name]
    (str a-name " sent " (pr-str value) " to " (pr-str key)
         " which was polled by " b-name)))

(defn wr-graph
  "Analyzes a history to extract write-read dependencies. T1 < T2 iff T1 writes
  some v to k and T2 reads k."
  [{:keys [writer-of readers-of]} history]
  {:graph (loopr [g (g/linear (g/digraph))]
                 [[k v->readers] readers-of
                  [v readers]    v->readers]
                 (if-let [writer (-> writer-of (get k) (get v))]
                   (let [readers (remove #{writer} readers)]
                     (recur (g/link-to-all g writer readers)))
                   (throw+ {:type :no-writer-of-value, :key k, :value v}))
                 (g/named-graph :wr (g/forked g)))
   :explainer (WRExplainer. writer-of)})

(defn graph
  "A combined Elle dependency graph between completion operations."
  [analysis history]
  ((elle/combine (partial ww-graph analysis)
                 (partial wr-graph analysis)
                 ;(partial rw-graph analysis))
                 )
   history))

(defn cycles!
  "Finds a map of cycle names to cyclic anomalies in a partial analysis."
  [{:keys [history directory] :as analysis}]
  ; Bit of a hack--our tests leave off :type fairly often, so we don't bother
  ; running this analysis for those tests.
  (when (:type (first history))
    (let [opts (cond-> {:consistency-models [:strict-serializable]}
                 (contains? analysis :directory)
                 (assoc  :directory (str directory "/elle")))
          ; For our purposes, we only want to infer cycles over txn/poll/send
          ; ops
          history  (h/filter (h/has-f? #{:txn :poll :send}) history)
          analyzer (->> opts
                        txn/additional-graphs
                        (into [(partial graph analysis)])
                        (apply elle/combine))]
      (:anomalies (txn/cycles! opts analyzer history)))))

(defn analysis
  "Builds up intermediate data structures used to understand a history. Options
  include:

  :directory - Used for generating output files
  :ww-deps   - Whether to perform write-write inference on the basis of log
               offsets."
  ([history]
   (analysis history {}))
  ([history opts]
  (let [history               (h/client-ops history)
        writes-by-type        (future (writes-by-type history))
        reads-by-type         (future (reads-by-type history))
        version-orders        (future (version-orders history @reads-by-type))
        writer-of             (future (writer-of history))
        readers-of            (future (readers-of history))
        ; Sort of a hack; we only bother computing this for "real" histories
        ; because our test suite often leaves off processes and times
        realtime-lag          (future
                                (let [op (first history)]
                                  (when (and (:process op)
                                             (:time op))
                                    (realtime-lag history))))
        worst-realtime-lag    (future (worst-realtime-lag @realtime-lag))
        unseen                (future (unseen history))
        version-order-errors  (:errors @version-orders)
        version-orders        (:orders @version-orders)
        analysis              (assoc opts
                                     :history        history
                                     :writer-of      @writer-of
                                     :readers-of     @readers-of
                                     :writes-by-type @writes-by-type
                                     :reads-by-type  @reads-by-type
                                     :version-orders version-orders)
        g1a-cases               (future (g1a-cases analysis))
        lost-write-cases        (future (lost-write-cases analysis))
        poll-skip+nm-cases      (future (poll-skip+nonmonotonic-cases analysis))
        nonmonotonic-send-cases (future (nonmonotonic-send-cases analysis))
        int-poll-skip+nm-cases  (future (int-poll-skip+nonmonotonic-cases analysis))
        int-send-skip+nm-cases  (future (int-send-skip+nonmonotonic-cases analysis))
        duplicate-cases         (future (duplicate-cases analysis))
        cycles                  (future (cycles! analysis))
        poll-skip-cases         (:skip @poll-skip+nm-cases)
        nonmonotonic-poll-cases (:nonmonotonic @poll-skip+nm-cases)
        int-poll-skip-cases     (:skip @int-poll-skip+nm-cases)
        int-nm-poll-cases       (:nonmonotonic @int-poll-skip+nm-cases)
        int-send-skip-cases     (:skip @int-send-skip+nm-cases)
        int-nm-send-cases       (:nonmonotonic @int-send-skip+nm-cases)
        last-unseen             (-> (peek @unseen)
                                    (update :unseen
                                            (fn [unseen]
                                              (->> unseen
                                                   (filter (comp pos? val))
                                                   (into (sorted-map)))))
                                    (update :messages
                                            (fn [messages]
                                              (->> messages
                                                   (filter (comp seq val))
                                                   (into (sorted-map))))))
        ]
    {:errors (cond-> {}
               @duplicate-cases
               (assoc :duplicate @duplicate-cases)

               int-poll-skip-cases
               (assoc :int-poll-skip int-poll-skip-cases)

               int-nm-poll-cases
               (assoc :int-nonmonotonic-poll int-nm-poll-cases)

               int-nm-send-cases
               (assoc :int-nonmonotonic-send int-nm-send-cases)

               int-send-skip-cases
               (assoc :int-send-skip int-send-skip-cases)

               version-order-errors
               (assoc :inconsistent-offsets version-order-errors)

               @g1a-cases
               (assoc :G1a @g1a-cases)

               @lost-write-cases
               (assoc :lost-write @lost-write-cases)

               nonmonotonic-poll-cases
               (assoc :nonmonotonic-poll nonmonotonic-poll-cases)

               @nonmonotonic-send-cases
               (assoc :nonmonotonic-send @nonmonotonic-send-cases)

               poll-skip-cases
               (assoc :poll-skip poll-skip-cases)

               (seq (:unseen last-unseen))
               (assoc :unseen last-unseen)

               true
               (merge @cycles))
     :history            history
     :realtime-lag       @realtime-lag
     :worst-realtime-lag @worst-realtime-lag
     :unseen             @unseen
     :version-orders     version-orders})))

(defn condense-error
  "Takes a test and a  pair of an error type (e.g. :lost-write) and a seq of
  errors. Returns a pair of [type, {:count n, :errors [...]}], which tries to
  show the most interesting or severe errors without making the pretty-printer
  dump out two gigabytes of EDN."
  [test [type errs]]
  [type
   (case type
     :unseen (if (:all-errors test)
               errs
               (assoc errs :messages
                      (map-vals (comp (partial take 32) sort)
                                (:messages errs))))
     {:count (if (coll? errs) (count errs) 1)
      :errs
      (if (:all-errors test)
        errs
        (case type
          :duplicate             (take 32      (sort-by :count errs))
          (:G0, :G0-process, :G0-realtime,
           :G1c, :G1c-process, :G1c-realtime)
          (take 8  (sort-by (comp count :steps) errs))
          :inconsistent-offsets  (take 32 (sort-by (comp count :values) errs))
          :int-nonmonotonic-poll (take 8       (sort-by :delta errs))
          :int-nonmonotonic-send (take 8       (sort-by :delta errs))
          :int-poll-skip         (take-last 8  (sort-by :delta errs))
          :int-send-skip         (take-last 8  (sort-by :delta errs))
          :nonmonotonic-poll     (take 8       (sort-by :delta errs))
          :nonmonotonic-send     (take 8       (sort-by :delta errs))
          :poll-skip             (take-last 8  (sort-by :delta errs))
          errs))})])

(defn allowed-error-types
  "Redpanda does a lot of *things* that are interesting to know about, but not
  necessarily bad or against-spec. For instance, g0 cycles are normal in the
  Kafka transactional model, and g1c is normal with wr-only edges at
  read-uncommitted but *not* with read-committed. This is a *very* ad-hoc
  attempt to encode that so that Jepsen's valid/invalid results are somewhat
  meaningful.]

  Takes a test, and returns a set of keyword error types (e.g. :poll-skip)
  which this test considers allowable."
  [test]
  (cond-> #{; int-send-skip is normal behavior: transaction writes interleave
            ; constantly in the Kafka transaction model. We don't even bother
            ; looking at external send skips.
            :int-send-skip
            ; Likewise, G0 is always allowed, since writes are never isolated
            :G0 :G0-process :G0-realtime
            }

            ; With subscribe, we expect external poll skips and nonmonotonic
            ; polls, because our consumer might be rebalanced between
            ; transactions. Without subscribe, we expect consumers to proceed
            ; in order.
            (:subscribe (:sub-via test)) (conj :poll-skip :nonmonotonic-poll)

            ; When we include ww edges, G1c is normal--the lack of write
            ; isolation means we should expect cycles like t0 <ww t1 <wr t0.
            (:ww-deps test) (conj :G1c :G1c-process :G1c-realtime)
            ))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [dir (store/path! test)
            {:keys [errors
                    realtime-lag
                    worst-realtime-lag
                    unseen] :as analysis}
            (analysis history {:directory dir
                               :ww-deps   (:ww-deps test)})
            ; What caused our transactions to return indefinite results?
            info-txn-causes (->> history
                                 (filter (comp #{:info} :type))
                                 (filter (comp #{:txn :send :poll} :f))
                                 (map :error)
                                 distinct)
            ; Which errors are bad enough to invalidate the test?
            bad-error-types (->> (keys errors)
                                 (remove (allowed-error-types test))
                                 sort)]
        ; Render plots
        (render-order-viz!   test analysis)
        (plot-unseen!        test unseen opts)
        (plot-realtime-lags! test realtime-lag opts)
        ; Write out a file with consume counts
        (store/with-out-file test "consume-counts.edn"
          (pprint (consume-counts history)))
        ; Construct results
        (->> errors
             (map (partial condense-error test))
             (into (sorted-map))
             (merge {:valid?             (empty? bad-error-types)
                     :worst-realtime-lag (-> worst-realtime-lag
                                             (update :time nanos->secs)
                                             (update :lag nanos->secs))
                     :bad-error-types    bad-error-types
                     :error-types        (sort (keys errors))
                     :info-txn-causes    info-txn-causes}))))))

(defn stats-checker
  "Wraps a (jepsen.checker/stats) with a new checker that returns the same
  results, except it won't return :valid? false if :crash or
  :debug-topic-partitions ops always crash. You might want to wrap your
  existing stats checker with this."
  ([]
   (stats-checker (checker/stats)))
  ([c]
   (reify checker/Checker
     (check [this test history opts]
       (let [res (checker/check c test history opts)]
         (if (every? :valid? (vals (dissoc (:by-f res)
                                           :debug-topic-partitions
                                           :crash)))
           (assoc res :valid? true)
           res))))))

(defn workload
  "Constructs a workload (a map with a generator, client, checker, etc) given
  an options map. Options are:

    :crash-clients? If set, periodically emits a :crash operation which the
                    client responds to with :info; this forces the client to be
                    torn down and replaced by a fresh client.

    :crash-client-interval How often, in seconds, to crash clients. Default is
                           30 seconds.

    :sub-via        A set of subscription methods: either #{:assign} or
                    #{:subscribe}.

    :txn?           If set, generates transactions with multiple send/poll
                    micro-operations.

  These options must also be present in the test map, because they are used by
  the checker, client, etc at various points. For your convenience, they are
  included in the workload map returned from this function; merging that map
  into your test should do the trick.

  ... plus those taken by jepsen.tests.cycle.append/test, e.g. :key-count,
  :min-txn-length, ..."
  [opts]
  (let [workload (append/test
                   (assoc opts
                          ; TODO: don't hardcode these
                          :max-txn-length (if (:txn? opts) 4 1)))
        max-offsets (atom (sorted-map))]
    (-> workload
        (merge (select-keys opts [:crash-clients?
                                  :crash-client-interval
                                  :sub-via
                                  :txn?]))
        (assoc :checker         (checker)
               :final-generator (gen/each-thread (final-polls max-offsets))
               :generator       (gen/any
                                  (crash-client-gen opts)
                                  (->> (:generator workload)
                                       txn-generator
                                       tag-rw
                                       (track-key-offsets max-offsets)
                                       interleave-subscribes
                                       poll-unseen))))))
