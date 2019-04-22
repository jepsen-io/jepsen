(ns jepsen.tests.cycle
  "Tests based on detecting cycles between operations in a history.

  First, we define a bunch of ways you can compute a dependency graph over a
  history. Our graphs use completions (e.g. ok, info) for the canonical
  representation of an operation. For instance:

  `realtime-graph` relates a->b if operation b begins after operation a
  completes successfully.

  `process-graph` relates a->b if a and b are performed by the same process,
  and that process executes a before b.

  `monotonic-key-graph` assumes op :values are maps of keys to observed values,
  like {:x 1 :y 2}. It relates a->b if b observed a higher value of some key
  than a did. For instance, {:x 1 :y 2} -> {:x 2 :y 2}, because :x is higher in
  the second op.

  `wr-graph` assumes op :values are transactions like [[f k v] ...], and
  relates operations based on transactional reads and writes: a->b if b
  observes a value that a wrote. This order requires that keys only have a
  given value written once.

  You can also *combine* graphs using `combine`, which takes the union of
  multiple graphs. `(combine realtime monotonic-key)`, for instance, verifies
  that not only must the values of each key monotonically increase, but that
  those increases must be observed in real time: stale reads are not allowed.

  Given a function which takes a history and produces a dependency graph, the
  checker runs that function, identifies strongly-connected components in the
  resulting graph, and decides whether the history is valid based on whether
  the graph has any strongly connected components--e.g. cycles."
  (:require [jepsen [checker :as checker]
                    [generator :as gen]
                    [txn :as txn]
                    [util :as util]]
            [jepsen.txn.micro-op :as mop]
            [knossos [op :as op]
                     [history :refer [pair-index]]]
            [clojure.tools.logging :refer [info error warn]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [fipp.edn :refer [pprint]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (io.lacuna.bifurcan DirectedGraph
                               Graphs
                               ICollection
                               IList
                               ISet
                               IGraph
                               Set
                               SortedMap)))

; Convert stuff back to Clojure data structures
(defprotocol ToClj
  (->clj [x]))

(extend-protocol ToClj
  IList
  (->clj [l]
    (iterator-seq (.iterator l)))

  ISet
  (->clj [s]
  (let [iter (.iterator s)]
    (loop [s (transient #{})]
      (if (.hasNext iter)
        (recur (conj! s (->clj (.next iter))))
        (persistent! s)))))

  IGraph
  (->clj [g]
    (->> (.vertices g)
         ->clj
         (map (fn [vertex] [vertex (->clj (.out g vertex))]))
         (into {})))

  clojure.lang.IPersistentMap
  (->clj [m]
    (into {} (map (fn [[k v]] [(->clj k) (->clj v)]) m)))

  Object
  (->clj [x] x)

  nil
  (->clj [x] x))

(defn forked
  "Bifurcan's analogue to (persistent! g)"
  [^DirectedGraph graph]
  (.forked graph))

(defn in
  "Inbound edges to a graph."
  [^IGraph g v]
  (try (.in g v)
       (catch IllegalArgumentException e)))

(defn link
  "Helper for linking Bifurcan graphs."
  [^DirectedGraph graph node succ]
  (assert (not (nil? node)))
  (assert (not (nil? succ)))
  (.link graph node succ))

(defn link-to-all
  "Given a graph g, links x to all ys."
  [g x ys]
  (if (seq ys)
    (recur (link g x (first ys)) x (next ys))
    g))

(defn link-all-to-all
  "Given a graph g, links all xs to all ys."
  [g xs ys]
  (if (seq xs)
    (recur (link-to-all g (first xs) ys) (next xs) ys)
    g))

(defn map->bdigraph
  "Turns a sequence of [node, successors] pairs (e.g. a map) into a bifurcan
  directed graph"
  [m]
  (reduce (fn [^DirectedGraph g [node succs]]
            (reduce (fn [graph succ]
                      (link graph node succ))
                    g
                    succs))
          (.linear (DirectedGraph.))
          m))

(defn ^Set ->bset
  "Turns any collection into a Bifurcan Set."
  ([coll]
   (->bset coll (.linear (Set.))))
  ([coll ^Set s]
   (if (seq coll)
     (recur (next coll) (.add s (first coll)))
     s)))

(defn ^DirectedGraph digraph-union
  "Takes the union of n graphs."
  ([] (DirectedGraph.))
  ([a] a)
  ([^DirectedGraph a ^DirectedGraph b]
   (.merge a b))
  ([a b & more]
   (reduce digraph-union a (cons b more))))

(defn strongly-connected-components
  "Finds the strongly connected components of a graph, greater than 1 element."
  [g]
  (map ->clj (Graphs/stronglyConnectedComponents g false)))

(defn tarjan
  "Returns the strongly connected components of a graph specified by its
  nodes (ints) and a successor function (succs node) from node to nodes.
  A iterative verison of Tarjan's Strongly Connected Components."
  [graph]
  (strongly-connected-components (map->bdigraph graph)))

; This is going to look a bit odd. Please bear with me.
;
; Our analysis generally goes like this:
;
;   1. Take a history, and analyze it to build a dependency graph.
;   2. Find cycles in that graph
;   3. Explain why those are cycles
;
; Computing the graphs is fairly straightforward, but explaining cycles is a
; bit less so, because the explanation may require data that's expensive to
; calculate. For instance, realtime orders would like to be able to tell you
; when precisely a given ok operation was invoked, and that needs an expensive
; index to be constructed over the full history. We want to be able to *re-use*
; that expensive state.
;
; At the top level, we want to have a single object that defines how to analyze
; a history *and* how to explain why the results of that analysis form cycles.
; We also want to be able to *compose* those objects together. That means that
; both graph construction and cycle explanation must compose.
;
; To do this, we decouple analysis into three objects:
;
; - An Analyzer, which which examines a history and produces a pair of:
; - A Graph of dependencies over completion ops, and
; - An Explainer, which can explain cycles between those ops.
;
; We write our Analyzers as a function (f history) => [graph explainer]. Graphs
; are Bifurcan DirectedGraphs. Explainers have a protocol, because we're going
; to pack some cached state into them.
(defprotocol Explainer
  (explain-pair
    [_ a b]
    "Given a pair of operations, explain why b depends on a. `nil` indicates
    that b does not depend on a."))

(defrecord CombinedExplainer [explainers]
  Explainer
  (explain-pair [_ a b]
    (first (keep (fn [e] (explain-pair e a b)) explainers))))

(defn combine
  "Helpful in composing an analysis function for a checker out of multiple
  other analysis fns. For example, you might want a checker that looks for
  per-key monotonicity *and* real-time precedence---you could use:

  (checker (combine monotonic-keys real-time))"
  [& analyzers]
  (fn [history]
    (let [[graph explainers]
          (reduce (fn [[graph explainers] analyzer]
                    (let [[g e] (analyzer history)]
                      [(digraph-union graph g) (conj explainers e)]))
                  [(.linear (DirectedGraph.)) []]
                  analyzers)]
      [(.forked graph) (CombinedExplainer. explainers)])))

;; Monotonic keys!

(defrecord MonotonicKeyExplainer []
  Explainer
  (explain-pair [_ a b]
    (let [a (:value a)
          b (:value b)]
      ; Find keys in common
      (->> (keys a)
           (filter b)
           (reduce (fn [_ k]
                     (let [a (get a k)
                           b (get b k)]
                       (when (and a b (< a b))
                         (reduced (str "which observed " (pr-str k) " = "
                                       (pr-str a)
                                       ", and a higher value " (pr-str b)
                                       " was observed by")))))
                   nil)))))

(defn monotonic-key-order
  "Given a key, and a history where ops are maps of keys to values, constructs
  a partial order graph over ops reading successive values of key k."
  [k history]
  ; Construct an index of values for k to all ops with that value.
  (let [index (as-> history x
                (group-by (fn [op] (get (:value op) k ::not-found)) x)
                (dissoc x ::not-found))]
    (->> index
         ; Take successive pairs of keys
         (sort-by key)
         (partition 2 1)
         ; And build a graph out of them
         (reduce (fn [g [[v1 ops1] [v2 ops2]]]
                   (link-all-to-all g ops1 ops2))
                 (.linear (DirectedGraph.)))
         forked)))

(defn monotonic-key-graph
  "Analyzes ops where the :value of each op is a map of keys to values. Assumes
  keys are monotonically increasing, and derives relationships between ops
  based on those values."
  [history]
  (let [history (filter op/ok? history)
        graph (->> history
                   (mapcat (comp keys :value))
                   distinct
                   (map (fn [k] (monotonic-key-order k history)))
                   (reduce digraph-union))]
    [graph (MonotonicKeyExplainer.)]))

;; Processes

(defn process-order
  "Given a history and a process ID, constructs a partial order graph based on
  all operations that process performed."
  [history process]
  (->> history
       (filter (comp #{process} :process))
       (partition 2 1)
       (reduce (fn [g [op1 op2]] (link g op1 op2))
               (.linear (DirectedGraph.)))
       forked))

(defrecord ProcessExplainer []
  Explainer
  (explain-pair [_ a b]
    (when (and (= (:process a) (:process b))
               (< (:index a) (:index b)))
      (str "which process " (:process a) " completed before"))))

(defn process-graph
  "Analyses histories and relates operations performed sequentially by each
  process, such that every operation a process performs is ordered (but
  operations across different processes are not related)."
  [history]
  (let [oks (filter op/ok? history)
       graph (->> oks
                  (map :process)
                  distinct
                  (map (fn [p] (process-order oks p)))
                  (reduce digraph-union))]
    [graph (ProcessExplainer.)]))

; Realtime order

(defrecord RealtimeExplainer [pairs]
  Explainer
  (explain-pair [_ a' b']
    (let [b (get pairs b')]
      (when (< (:index a') (:index b))
        (str "which completed"
             (when (and (:time a') (:time b))
               (str (format "%.3f" (util/nanos->secs (- (:time b) (:time a'))))
                    " seconds "))
             " before the invocation of")))))

(defn realtime-graph
  "Given a history, produces an singleton order graph `{:realtime graph}` which
  encodes the real-time dependencies of transactions: a < b if a ends before b
  begins.

  In general, txn a precedes EVERY txn which begins later in the history, but
  that's n^2 territory, and for purposes of cycle detection, we only need a
  transitive reduction of that graph. We compute edges from txn a to subsequent
  invocations until a new operation b completes."
  [history]
  ; Our basic approach here is to iterate through the history, and for any
  ; given :ok op, create an edge from that :ok to the :ok or :info
  ; corresponding to subsequent :invokes--because our graph relates
  ; *completion* operations, not invocations.
  ;
  ; What about this history?
  ;
  ;   ==a==       ==c== ==e==
  ;         ==b==
  ;           ==d===
  ;
  ; Do we need an edge from a->c? No, because b->c has it covered.
  ;
  ; What about e? a, b, c, and d precede e, but since a->b and b->c, we only
  ; need c->e and d->e.
  ;
  ; So... our strategy is to keep a buffer of all previous completions that
  ; need to be linked forward to new invocations. When we see an invoke x, we
  ; link every buffered completion (a, b, c...) to x. When we see a new
  ; completion d, we look backwards in the graph to see whether any buffered
  ; completions point to d, and remove those from the buffer.
  ;
  ; OK, first up: we need this index to look forward from invokes to completes.
  (let [pairs (pair-index history)]
    (loop [history history
           oks     #{}               ; Our buffer of completed ops
           g       (DirectedGraph.)] ; Our order graph
      (if-let [op (first history)]
        (case (:type op)
          ; A new operation begins! Link every completed op to this one's
          ; completion. Note that we generate edges here regardless of whether
          ; this op will fail or crash--we might not actually NEED edges to
          ; failures, but I don't think they'll hurt. We *do* need edges to
          ; crashed ops, because they may complete later on.
          :invoke (let [op' (get pairs op)
                        g   (reduce (fn [g ok] (link g ok op')) g oks)]
                    (recur (next history) oks g))
          ; An operation has completed. Add it to the oks buffer, and remove
          ; oks that this ok implies must have completed.
          :ok     (let [implied (->clj (in g op))
                        oks     (-> oks
                                    (set/difference implied)
                                    (conj op))]
                    (recur (next history) oks g))
          ; An operation that failed doesn't affect anything--we don't generate
          ; dependencies on failed transactions because they didn't happen. I
          ; mean we COULD, but it doesn't seem useful.
          :fail (recur (next history) oks g)
          ; Crashed operations, likewise: nothing can follow a crashed op, so
          ; we don't need to add them to the ok set.
          :info (recur (next history) oks g))
        ; All done!
        [g (RealtimeExplainer. pairs)]))))

; Adya-style dependency graphs over transactions

(defn appends-and-reads-verify-mop-types
  "Takes a history where operation values are transactions. Verifies that the
  history contains only reads [:r k v] and appends [:append k v]. Returns nil if the history conforms, or throws an error object otherwise."
  [history]
  (let [bad (remove (fn [op] (every? #{:r :append} (map mop/f (:value op))))
                    history)]
    (when (seq bad)
      (throw+ {:type      :unexpected-txn-micro-op-types
               :message   "history contained operations other than appends or reads"
               :examples  (take 8 bad)}))))

(defn prefix?
  "Given two sequences, returns true iff A is a prefix of B."
  [a b]
  (and (<= (count a) (count b))
       (loop [a a
              b b]
         (cond (nil? (seq a))           true
               (= (first a) (first b))  (recur (next a) (next b))
               true                     false))))

(defn appends-and-reads-sorted-values
  "Takes a history where operation values are transactions, and every micro-op
  in a transaction is an append or a read. Computes a map of keys to all
  distinct observed values for that key, ordered by length."
  [history]
  (->> history
       ; Build up a map of keys to sets of observed values for those keys
       (reduce (fn [states op]
                 (reduce (fn [states [f k v]]
                           (if (= :r f)
                             ; Good, this is a read
                             (-> states
                                 (get k #{})
                                 (conj v)
                                 (->> (assoc states k)))
                             ; Something else!
                             states))
                         states
                         (:value op)))
               {})
       ; If a total order exists, we should be able
       ; to sort their values by size and each one
       ; will be a prefix of the next.
       (util/map-vals (partial sort-by count))))

(defn appends-and-reads-verify-total-order
  "Takes a map of keys to observed values (e.g. from
  `appends-and-reads-sorted-values`, and verifies that for each key, the values
  read are consistent with a total order of appends. For instance, these values
  are consistent:

     {:x [[1] [1 2 3]]}

  But these two are not:

     {:x [[1 2] [1 3 2]]}

  ... because the first is not a prefix of the second.

  Returns nil if the history is OK, or throws otherwise."
  [sorted-values]
  (let [; For each key, verify the total order.
        errors (keep (fn ok? [[k values]]
                       (->> values
                            ; If a total order exists, we should be able
                            ; to sort their values by size and each one
                            ; will be a prefix of the next.
                            (partition 2 1)
                            (reduce (fn mop [error [a b]]
                                      (when-not (prefix? a b)
                                        (reduced
                                          {:key    k
                                           :values [a b]})))
                                    nil)))
                     sorted-values)]
    (when (seq errors)
      (throw+ {:type    :no-total-state-order
               :message "observed mutually incompatible orders of appends"
               :errors  errors}))))

(defn appends-and-reads-append-index
  "Takes a map of keys to observed values (e.g. from
  appends-and-reads-sorted-values), and builds a map of keys to indexes on
  those keys, where each index is a map relating appended values on that key to
  the order in which they were effectively appended by the database.

    {:x {:indices {v0 0, v1 1, v2 2}
         :values  [v0 v1 v2]}}

  This allows us to map bidirectionally."
  [sorted-values]
  (util/map-vals (fn [values]
                   ; The last value will be the longest, and since every other
                   ; is a prefix, it includes all the information we need.
                   (let [vs (util/fast-last values)]
                     {:values  vs
                      :indices (into {} (map vector vs (range)))}))
                 sorted-values))

(defn appends-and-reads-write-index
  "Takes a history restricted to oks and infos, and constructs a map of keys to
  append values to the operations that appended those values."
  [history]
  (reduce (fn [index op]
            (reduce (fn [index [f k v]]
                      (if (= :append f)
                        (assoc-in index [k v] op)
                        index))
                    index
                    (:value op)))
          {}
          history))

(defn appends-and-reads-read-index
  "Takes a history restricted to oks and infos, and constructs a map of keys to
  append values to the operations which observed the state generated by the
  append of k. The special append value ::init generates the initial (nil)
  state."
  [history]
  (reduce (fn [index op]
            (reduce (fn [index [f k v]]
                      (if (= :r f)
                        (update-in index [k (or (peek v) ::init)] conj op)
                        index))
                    index
                    (:value op)))
          {}
          history))

(defrecord AppendsAndReadsExplainer []
  Explainer
  (explain-pair [_ a b]
    "something something"))

(defn appends-and-reads-graph
  "Some parts of a transaction's dependency graph--for instance,
  anti-dependency cycles--involve the *version order* of states for a key.
  Given two transactions: [[:w :x 1]] and [[:w :x 2]], we can't tell whether T1
  or T2 happened first. This makes it hard to identify read-write and
  write-write edges, because we can't tell what particular transaction should
  have overwritten the state observed by a previous transaction.

  However, if we constrain ourselves to transactions whose only mutation is to
  *append* a value to a key's current state...

    {:f :txn, :value [[:r :x [1 2]] [:append :x 3] [:r :x [1 2 3]]]} ...

  ... we can derive the version order for (almost) any pair of operations on
  the same key because the order of appends is encoded in every read: if we
  observe [:r :x [3 1 2]], we know the previous states must have been [3] [3 1]
  [3 1 2].

  That is, assuming appends actually work correctly. If the database loses
  appends or reorders them, it's *likely* (but not necessarily the case), that
  we'll observe states like:

    [1 2 3]
    [1 3 4]  ; 2 has been lost!

  We can verify these in a single O(appends^2) pass by sorting all observed
  states for a key by size, and verifying that each is a prefix of the next.
  Assuming we *do* observe a total order, we can use the longest read value for
  each key as an order over appends for that key. This order is *almost*
  complete in that appends which are never read can't be related, but so long
  as the DB lets us see most of our appends at least once, this should work.

  So then, our strategy here is to compute those orders for each key, then use
  them to relate successive [w w], [r w], and [w r] pair on that key. [w,w]
  pairs are a direct write dependency, [w,r] pairs are a direct
  read-dependency, and [r,w] pairs are direct anti-dependencies.

  For more context, see Adya, Liskov, and O'Neil's 'Generalized Isolation Level
  Definitions', page 8."
  ([history]
   ; Make sure there are only appends and reads
   (appends-and-reads-verify-mop-types history)
   ; We only care about ok and info ops; invocations don't matter, and failed
   ; ops can't contribute to the state.
   (let [history       (filter (comp #{:ok :info} :type) history)
         sorted-values (appends-and-reads-sorted-values history)]
     (prn :sorted-values sorted-values)
     ; Make sure we can actually compute a version order for each key
     (appends-and-reads-verify-total-order sorted-values)
     ; Compute indices
     (let [append-index (appends-and-reads-append-index sorted-values)
           write-index  (appends-and-reads-write-index history)
           read-index   (appends-and-reads-read-index history)
           ; And build our graph
           _     (prn :append-index append-index)
           _     (prn :write-index  write-index)
           _     (prn :read-index   read-index)
           graph (appends-and-reads-graph
                   history append-index write-index read-index)]
       [graph (AppendsAndReadsExplainer.)])))
  ; Actually build the DirectedGraph
  ([history append-index write-index read-index]
   (reduce
     (fn op [g op]
       (prn :op (:value op))
       (reduce
         (fn mop [g [f k v]]
           (prn :mop f k v)
           (case f
             ; OK, we've got a read. What op appended the last thing we saw?
             :r (if (seq v)
                  (let [_ (prn :last-v (peek v))
                        append-op (-> write-index (get k) (get (peek v)))]
                    (prn :appender append-op)
                    (assert append-op)
                    (prn :link append-op '-> op)
                    (if (= op append-op)
                      ; We appended this particular value, and that append will
                      ; encode the w-w dependency we need for this read.
                      g
                      ; We depend on the other op that appended this.
                      (link g append-op op)))
                  ; The read value was empty; we don't depend on anything.
                  g)

             ; OK, we have an append. We need two classes of edge here: one ww
             ; edge, for the append we're overwriting, and n rw edges, for
             ; anyone who read the version immediately prior to us.
             :append
             ; So what version did we append?
             (if-let [index (get-in append-index [k :indices v])]
               (let [; And what value was appended immediately before us in
                     ; version order?
                     prev-v (if (pos? index)
                              (get-in append-index [k :values (dec index)])
                              ::init)
                     _ (prn :prev-v prev-v)

                     ; What op wrote that value?
                     prev-append (when (pos? index)
                                   (get-in write-index [k prev-v]))

                     ; Link that writer to us
                     g (if (and (pos? index)             ; (if we weren't first)
                                (not= prev-append op)) ; (and it wasn't us)
                         (link g prev-append op)
                         g)

                     ; And what ops read that previous value?
                     prev-reads (get-in read-index [k prev-v])

                     ; Link them all to us
                     g (reduce (fn link-read [g r]
                                 (if (not= r op)
                                   (link g r op)
                                   g))
                               g
                               prev-reads)]
                 g)
               ; Huh, we actually don't know what index this was--because we
               ; never read this appended value. Nothing we can infer!
               g)))
         g
         (:value op)))
     (.linear (DirectedGraph.))
     history)))

; Basic kv reads and writes

(defn ext-index
  "Given a function that takes a txn and returns a map of external keys to
  written values for that txn, and a history, computes a map like {k {v [op1,
  op2, ...]}}, where k is a key, v is a particular value for that key, and op1,
  op2, ... are operations which externally wrote k=v.

  Right now we index only :ok ops. Later we should do :infos too, but we need
  to think carefully about how to interpret the meaning of their nil reads."
  [ext-fn history]
  (->> history
       (r/filter op/ok?)
       (reduce (fn [idx op]
                 (reduce (fn [idx [k v]]
                           (update-in idx [k v] conj op))
                         idx
                         (ext-fn (:value op))))
               {})))

(defrecord WRExplainer []
  Explainer
  (explain-pair [_ a b]
    (let [writes (txn/ext-writes (:value a))
          reads  (txn/ext-reads  (:value b))]
      (reduce (fn [_ k]
                (let [r (get reads k)
                      w (get writes k)]
                  (when (= r w)
                    (reduced
                      (str "which wrote " (pr-str k) " = " (pr-str w)
                           ", which was read by")))))
              nil
              (keys reads)))))

(defn wr-graph
  "Given a history where ops are txns (e.g. [[:r :x 2] [:w :y 3]]), constructs
  an order over txns based on the external writes and reads of key k: any txn
  that reads value v must come after the txn that wrote v."
  [history]
  (let [ext-writes (ext-index txn/ext-writes  history)
        ext-reads  (ext-index txn/ext-reads   history)]
    ; Take all reads and relate them to prior writes.
    [(.forked
       (reduce (fn [graph [k values->reads]]
                 ; OK, we've got a map of values to ops that read those values
                 (reduce (fn [graph [v reads]]
                           ; Find ops that set k=v
                           (let [writes (-> ext-writes (get k) (get v))]
                             (case (count writes)
                               ; Huh. We read a value that came out of nowhere.
                               ; This is probably an initial state. Later on
                               ; we could do something interesting here, like
                               ; enforcing that there's only one of these
                               ; values and they have to precede all writes.
                               0 graph

                               ; OK, in this case, we've got exactly one
                               ; txn that wrote this value, which is good!
                               ; We can generate dependency edges here!
                               1 (link-to-all graph (first writes) reads)

                               ; But if there's more than one, we can't do this
                               ; sort of cycle analysis because there are
                               ; multiple alternative orders. Technically, it'd
                               ; be legal to ignore these, but I think it's
                               ; likely the case that users will want a big
                               ; flashing warning if they mess this up.
                               (assert (< (count writes) 2)
                                       (throw (IllegalArgumentException.
                                                (str "Key " (pr-str k)
                                                     " had value " (pr-str v)
                                                     " written by more than one op: "
                                                     (pr-str writes))))))))
                         graph
                         values->reads))
               (.linear (DirectedGraph.))
               ext-reads))
     (WRExplainer.)]))

(defn find-cycle
  ([^IGraph graph scc]
   (-> graph
       (.select (->bset scc)) ; Restrict the graph to this particular scc
       (Graphs/cycles)
       (->> (sort-by #(.size ^ICollection %)))
       first
       ->clj)))

(defn explain-scc
  "Takes a graph, an explainer, and a strongly connected component (a
  collection of ops) in that graph. Using those graphs, constructs a chain
  like:

      [op1 relationship op2 relationship ... op1]

  ... illustrating how they form a cycle. For instance:

      [{:process 0 :value {:x 5}}
       [:process 0] {:process 0 :value {:x 3}}
       [:key :x]    {:process 1 :value {:x 4}}
       [:key :x]    {:process 0 :value {:x 5}}]"
  [graph explainer scc]
  (let [cycle (find-cycle graph scc)]
    (->> cycle
         (partition 2 1)
         (reduce (fn [explanation [a b]]
                   (if-let [e (explain-pair explainer a b)]
                     (conj explanation (explain-pair explainer a b) b)
                     ; uhhhh
                     (throw (IllegalStateException.
                              (str "Explainer " (pr-str explainer)
                                   " was unable to explain the relationship"
                                   " between" (pr-str a) " and " (pr-str b))))))
                 [(first cycle)]))))

(defn checker
  "Takes a function which takes a history and returns a [graph, explainer]
  pair, and returns a checker which uses those graphs to identify cyclic
  dependencies."
  [analyze-fn]
  (reify checker/Checker
    (check [this test history opts]
      (let [history           (remove (comp #{:nemesis} :process) history)
            [graph explainer] (analyze-fn history)
            sccs              (strongly-connected-components graph)]
         {:valid?     (empty? sccs)
          :cycles     (->> sccs
                           (sort-by count)
                           (map (partial explain-scc graph explainer)))}))))
