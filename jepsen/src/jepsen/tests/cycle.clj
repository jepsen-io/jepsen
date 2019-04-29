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
                     [history :refer [pair-index+]]]
            [clojure.tools.logging :refer [info error warn]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.string :as str]
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
    [_ a-name a b-name b]
    "Given a pair of operations, and short names for them, explain why b
    depends on a. `nil` indicates that b does not depend on a."))

(defrecord CombinedExplainer [explainers]
  Explainer
  (explain-pair [_ a-name a b-name b]
    (first (keep (fn [e] (explain-pair e a-name a b-name b)) explainers))))

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
  (explain-pair [_ a-name a b-name b]
    (let [a (:value a)
          b (:value b)]
      ; Find keys in common
      (->> (keys a)
           (filter b)
           (reduce (fn [_ k]
                     (let [a (get a k)
                           b (get b k)]
                       (when (and a b (< a b))
                         (reduced (str a-name " observed " (pr-str k) " = "
                                       (pr-str a) ", and " b-name
                                       " observed a higher value "
                                       (pr-str b))))))
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
  (explain-pair [_ a-name a b-name b]
    (when (and (= (:process a) (:process b))
               (< (:index a) (:index b)))
      (str "process " (:process a) " executed " a-name " before " b-name))))

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
  (explain-pair [_ a'-name a' b'-name b']
    (let [b (get pairs b')]
      (when (< (:index a') (:index b))
        (str a'-name " completed "
             (when (and (:time a') (:time b))
               (str (format "%.3f" (util/nanos->secs (- (:time b) (:time a'))))
                    " seconds "))
             " before the invocation of " b'-name)))))

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
  (let [pairs (pair-index+ history)]
    (loop [history history
           oks     #{}               ; Our buffer of completed ops
           g       (DirectedGraph.)] ; Our order graph
      (when-let [op (first history)]
        (info :op op :pair (get pairs op)))
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
  (explain-pair [_ a-name a b-name b]
    (let [writes (txn/ext-writes (:value a))
          reads  (txn/ext-reads  (:value b))]
      (reduce (fn [_ k]
                (let [r (get reads k)
                      w (get writes k)]
                  (when (= r w)
                    (reduced
                      (str a-name " wrote " (pr-str k) " = " (pr-str w)
                           ", which was read by " b-name)))))
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

; Hooo boy, this blows up BADLY on real-world inputs, so we're gonna hack
; together a little BFS
;(defn find-cycle
;  ([^IGraph graph scc]
;   (-> graph
;       (.select (->bset scc)) ; Restrict the graph to this particular scc
;       (Graphs/cycles)
;       (->> (sort-by #(.size ^ICollection %)))
;       first
;       ->clj)))

(defn prune-alternate-paths
  "If we have two paths [a b d] and [a c d], we don't need both of them,
  because both get us from a to d. We collapse a set of paths by filtering out
  duplicates. Since all paths *start* at the same place, we can do this
  efficiently by selecting one from the set of all paths that end in the same
  place."
  [paths]
  (->> paths
       (group-by peek)
       vals
       (map first)))

(defn prune-longer-paths
  "We can also completely remove paths whose tips are in a prefix of any other
  path, because these paths are simply longer versions of paths we've already
  explored."
  [paths]
  (let [old (->> paths
                 (mapcat butlast)
                 (into #{}))]
    (remove (comp old peek) paths)))

(defn grow-paths
  "Given a graph g, and a set of paths (each a sequence like [a b c]),
  constructs a new set of paths by taking the tip c of each path p, and
  expanding p to all vertices c can reach: [a b c] => #{[a b c d] [a b c e]}."
  [^DirectedGraph g, paths]
  (->> paths
       (mapcat (fn [path]
                 (let [tip (peek path)]
                   ; Expand the tip to all nodes it can reach
                   (map (partial conj path) (.out g tip)))))))

(defn path-shells
  "Given a graph, and a starting node, constructs shells outwards from that
  node: collections of longer and longer paths."
  [g start]
  (iterate (comp prune-alternate-paths
                 (partial grow-paths g)
                 prune-longer-paths)
           [[start]]))

(defn loop?
  "Does the given vector begin and end with identical elements, and is longer
  than 1?"
  [v]
  (and (< 1 (count v))
       (= (first v) (peek v))))

(defn renumber-graph
  "Takes a Graph and rewrites each vertex to a unique integer, returning the
  rewritten Graph, and a vector of the original vertexes for reconstruction."
  [g]
  (let [mapping (persistent!
                  (reduce (fn [mapping v]
                            (assoc! mapping v (count mapping)))
                          (transient {})
                          (.vertices g)))
        g' (reduce (fn [g' v]
                     ; Find the index of this vertex
                     (let [vi (get mapping v)]
                       (reduce (fn [g' n]
                                 ; And the index of this neighbor
                                 (let [ni (get mapping n)]
                                   (link g' vi ni)))
                               g'
                               (.out g v))))
                (.linear (DirectedGraph.))
                (.vertices g))]
    [(.forked g')
     (persistent!
       (reduce (fn [vertices [vertex index]]
                 (assoc! vertices index vertex))
               (transient (vec (repeat (count mapping) nil)))
               mapping))]))

(defn find-cycle
  "Given a graph and a strongly connected component within it, finds a short
  cycle in that component."
  [^IGraph graph scc]
  (let [g             (.select graph (->bset scc))
        ; Just to speed up equality checks here, we'll construct an isomorphic
        ; graph with integers standing for our ops, find a cycle in THAT, then
        ; map back to our own space.
        [gn mapping]  (renumber-graph g)
        candidate     (first (.vertices gn))
        paths         (mapcat identity (path-shells gn candidate))]
    (->> paths
         (filter loop?)
         first
         (mapv mapping))))

(defn explain-binding
  "Takes a seq of [name op] pairs, and constructs a string naming each op."
  [bindings]
  (str "Let...\n"
       (->> bindings
            (map (fn [[name txn]]
                   (str "  " name " = " (pr-str txn))))
            (str/join "\n"))))

(defn explain-cycle-pair
  "Takes an explainer and a pair of [[a-name a] [b-name b] transactions, and
  for each one, constructs a string explaining why a precedes b."
  [explainer [[a-name a] [b-name b]]]
  (or (when-let [ex (explain-pair explainer a-name a b-name b)]
        (str a-name " < " b-name ", because " ex))
      ; uhhhh
      (throw (IllegalStateException.
               (str "Explainer " (pr-str explainer)
                    " was unable to explain the relationship"
                    " between " (pr-str a)
                    " and " (pr-str b))))))

(defn explain-cycle
  "Takes an explainer and a sequence of [op-name op] operations, and produces a
  string explaining why they follow in that order."
  [explainer ops]
  (let [explanations (->> (partition 2 1 ops)
                          (map (partial explain-cycle-pair explainer)))
        prefix       "  - "]
    (->> explanations
         (map-indexed (fn [i ex]
                        (if (= i (dec (count explanations)))
                          (str prefix " However, " ex ": a contradiction!")
                          (str prefix ex ".\n"))))
         str/join)))

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
  (let [cycle   (find-cycle graph scc)
        binding (->> (butlast cycle)
                     (map-indexed (fn [i op] [(str "T" (inc i)) op])))]
    (str (explain-binding binding)
         "\n\nThen:\n"
         (explain-cycle explainer (concat binding [(first binding)])))))

(defn checker
  "Takes a function which takes a history and returns a [graph, explainer]
  pair, and returns a checker which uses those graphs to identify cyclic
  dependencies."
  [analyze-fn]
  (reify checker/Checker
    (check [this test history opts]
      (try+
        (let [history           (remove (comp #{:nemesis} :process) history)
              [graph explainer] (analyze-fn history)
              sccs              (strongly-connected-components graph)]
          {:valid?     (empty? sccs)
           :scc-count  (count sccs)
           :cycles     (->> sccs
                            (sort-by (comp :index first))
                            (take 8)
                            (mapv (partial explain-scc graph explainer)))})
        ; The graph analysis might decide that a certain history isn't
        ; analyzable (perhaps because it violates expectations the analyzer
        ; needed to hold in order to infer dependencies), or it might discover
        ; violations that *can't* be encoded as a part of the dependency graph.
        ; If that happens, the analyzer can throw an exception with a :valid?
        ; key, and we'll simply return the ex-info map.
        (catch [:valid? false] e e)
        (catch [:valid? :unknown] e e)))))
