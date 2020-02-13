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
                    [store :as store]
                    [txn :as txn]
                    [util :as util :refer [spy]]]
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
                               Graphs$Edge
                               ICollection
                               IEdge
                               IList
                               ISet
                               IGraph
                               Set
                               SortedMap)
           (java.util.function BinaryOperator
                               Function)))

; Convert stuff back to Clojure data structures
(defprotocol ToClj
  (->clj [x]))

(extend-protocol ToClj
  IList
  (->clj [l]
    (iterator-seq (.iterator l)))

  IEdge
  (->clj [e]
    {:from  (.from e)
     :to    (.to e)
     :value (.value e)})

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

(defn digraph
  "Constructs a fresh directed graph."
  []
  (DirectedGraph.))

(defn linear
  "Bifurcan's analogue to (transient x)"
  [^ICollection x]
  (.linear x))

(defn forked
  "Bifurcan's analogue to (persistent! x)"
  [^ICollection x]
  (.forked x))

(defn in
  "Inbound edges to v in graph g."
  [^IGraph g v]
  (try (.in g v)
       (catch IllegalArgumentException e)))

(defn out
  "Outbound edges from v in graph g."
  [^IGraph g v]
  (try (.out g v)
       (catch IllegalArgumentException e)))

(def union-edge
  "A binary operator performing set union on the values of edges."
  (reify BinaryOperator
    (apply [_ a b]
      (set/union a b))))

(defn edge
  "Returns the edge between two vertices."
  [^IGraph g a b]
  (.edge g a b))

(defn edges
  "A lazy seq of all edges."
  [^IGraph g]
  ; We work around a bug in bifurcan which returns edges backwards!
  (map (fn [^IEdge e]
         (Graphs$Edge. (.value e) (.to e) (.from e)))
       (.edges g)))

(defn link
  "Helper for linking Bifurcan graphs. Optionally takes a relationship, which
  is added to the value set of the edge. Nil relationships are treated as if no
  relationship were passed."
  ([^DirectedGraph graph node succ]
   ;(assert (not (nil? node)))
   ;(assert (not (nil? succ)))
   (.link graph node succ))
  ([^DirectedGraph graph node succ relationship]
   (do ;(assert (not (nil? node)))
       ;(assert (not (nil? succ)))
       (.link graph node succ #{relationship} union-edge))))

(defn link-to-all
  "Given a graph g, links x to all ys."
  ([g x ys]
   (if (seq ys)
     (recur (link g x (first ys)) x (next ys))
     g))
  ([g x ys rel]
   (if (seq ys)
     (recur (link g x (first ys) rel) x (next ys) rel)
     g)))

(defn link-all-to
  "Given a graph g, links all xs to y."
  ([g xs y]
   (if (seq xs)
     (recur (link (first xs) y) (next xs) y)
     g))
  ([g xs y relationship]
   (if (seq xs)
     (recur (link g (first xs) y relationship) (next xs) y relationship)
     g)))

(defn link-all-to-all
  "Given a graph g, links all xs to all ys."
  ([g xs ys]
   (if (seq xs)
     (recur (link-to-all g (first xs) ys) (next xs) ys)
     g))
  ([g xs ys rel]
   (if (seq xs)
     (recur (link-to-all g (first xs) ys rel) (next xs) ys rel)
     g)))

(defn unlink
  "Heper for unlinking Bifurcan graphs."
  [^IGraph g a b]
  (.unlink g a b))

(defn unlink-to-all
  "Given a graph g, removes the link from x to all ys."
  [g x ys]
  (if (seq ys)
    (recur (unlink g x (first ys)) x (next ys))
    g))

(defn unlink-all-to
  "Given a graph g, removes the link from all xs to y."
  [g xs y]
  (if (seq xs)
    (recur (unlink g (first xs) y) (next xs) y)
    g))

(defn keep-edge-values
  "Transforms a graph by applying a function (f edge-value) to each edge in the
  graph. Where the function returns `nil`, removes that edge altogether."
  [f ^DirectedGraph g]
  (forked
     (reduce (fn [^IGraph g, ^IEdge edge]
              (let [v' (f (.value edge))]
                (if (nil? v')
                  ; Remove edge
                  (.unlink g (.from edge) (.to edge))
                  ; Update existing edge
                  (.link g (.from edge) (.to edge) v'))))
            (linear g)
            ; Note that we iterate over an immutable copy of g, and mutate a
            ; linear version in-place, to avoid seeing our own mutations during
            ; iteration.
            (edges (forked g)))))

(defn remove-relationship
  "Filters a graph, removing the given relationship from it."
  [g rel]
  (keep-edge-values (fn [rs]
                      (let [rs' (disj rs rel)]
                        (when (seq rs')
                          rs')))
                    g))

(defn project-relationship
  "Filters a graph to just those edges with the given relationship."
  [g rel]
  ; Might as well re-use this, we're gonna build it a lot.
  (let [rs' #{rel}]
    (keep-edge-values (fn [rs] (when (contains? rs rel) rs'))
                      g)))

(defn ^DirectedGraph remove-self-edges
  "There are times when it's just way simpler to use link-all-to-all between
  sets that might intersect, and instead of writing all-new versions of link-*
  that suppress self-edges, we'll just remove self-edges after."
  [^DirectedGraph g nodes-of-interest]
  (loop [g      (linear g)
         nodes  nodes-of-interest]
    (if (seq nodes)
      (let [node (first nodes)]
        (recur (unlink g node node) (next nodes)))
      (forked g))))

(defn downstream-matches
  "Returns the set of all nodes matching pred including or downstream of
  vertices. Takes a raw graph, and a memoized, which memoizes our previous
  findings. We use memo where possible, and fall back to raw graph searches
  otherwise."
  [pred g ^DirectedGraph memo vertices]
  (->> (reify Function
         (apply [_ node]
           (if (pred node)
             ; We're done!
             []
             (if (.contains (.vertices memo) node)
               ; Ah, good, we can jump right to memoized values
               (out memo node)

               ; Fall back to slow exploration via the raw graph
               (out g node)))))
       (Graphs/bfsVertices ^Iterable vertices)
       (filter pred)))

(defn ^DirectedGraph collapse-graph
  "Given a predicate function pred of a vertex, and a graph g, collapses g to
  just those vertices which satisfy (pred vertex), preserving transitive
  connections through removed vertices. If a -> b -> c, and we remove b, then a
  -> c.

  This method destroys relationship edges. I'm not sure what the semantics of
  preserving them might be, and we won't be using them for the application I'm
  thinking of anyway."
  [pred ^DirectedGraph g]
  ;(info "collapsing graph of " (.size g) "nodes," (count (filter pred (.vertices g))) "of which match pred")
  ; We proceed through the graph linearly, taking every node n which matches
  ; pred. We explore its downstream neighborhood up to and including, but not
  ; past, nodes matching pred. We add an edge from n to each downstream node to
  ; our result graph.
  ;
  ; We're going to take advantage of the fact that our graphs are probably
  ; roughly temporally ordered transactions by :index, and proceed in *reverse*
  ; index order to maximize the chances of hitting memoization.
  (->> (.vertices g)
       (sort-by :index)
       reverse
       (reduce (fn reducer [[g' memo] v]
                 ;(prn :v v :memo-size (.size (.vertices memo)))
                 (let [downstream (downstream-matches pred g memo (out g v))]
                   (if (pred v)
                     ; Good, build associations in our result graph
                     [(link-to-all g' v downstream) memo]
                     ; Well, this *wasn't* a node we were looking for... but we
                     ; can memoize it to speed up future searches.
                     [g' (-> memo
                             (.add v) ; Memoize even negative result!
                             (link-to-all v downstream))])))
               [(linear (digraph))
                (linear (digraph))])
       first
       forked))

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
  "Takes the union of n graphs, merging edges with union."
  ([] (DirectedGraph.))
  ([a] a)
  ([^DirectedGraph a ^DirectedGraph b]
   (.merge a b union-edge))
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

; We write our Analyzers as a function (f history) => [graph explainer]. Graphs
; are Bifurcan DirectedGraphs. Explainers have a protocol, because we're going
; to pack some cached state into them.

(defprotocol Anomalies
  "There are several points in an analysis where we want to construct some
  intermediate data, like a version graph, for later. However, it might be that
  *during* the construction of that version graph, we discover the graph itself
  contains anomalies. This doesn't *invalidate* the graph--perhaps we can still
  use it to discover anomalies later, but it *is* information we need to pass
  upstream to the user. The Anomalies protocol lets the version graph signal
  that additional anomalies were encountered, so whatever uses the version
  graph can merge those into its upstream anomaly set.

  This is kind of like an error monad, but it felt weird and less composable to
  use exceptions for this."
  (anomalies [this] "Returns a map of anomaly types to anomalies."))

(extend-protocol Anomalies
  ; nil means no anomalies
  nil (anomalies [_] nil)

  ; This lets us return plain old objects transparently when there's no
  ; anomalies.
  Object (anomalies [_] nil)

  ; It's convenient to just pass around a map when you do have anomalies, and
  ; don't need to encode extra information.
  clojure.lang.IPersistentMap
  (anomalies [m] m))

(defn merge-anomalies
  "Merges n Anomaly objects together."
  [coll-of-anomalies]
  (reify Anomalies
    (anomalies [_] (merge-with concat (map anomalies coll-of-anomalies)))))

(defprotocol TransactionGrapher
  "The TransactionGrapher takes a history and produces a TransactionGraph,
  optionally augmented with Anomalies discovered during the graph inference
  process."
  (build-transaction-graph [this history]
                           "Analyzes the history and returns a TransactionGraph,
                           optionally with Anomalies."))

(defprotocol TransactionGraph
  "A TransactionGraph represents a graph of dependencies between transactions
  (really, operations), where edges are sets of tagged relationships, like :ww
  or :realtime."
  (transaction-graph [this]
                     "Returns a Bifurcan IDirectedGraph of dependencies between
                     transactions (represented as completion operations)"))

(defprotocol DataExplainer
  (explain-pair-data
    [_ a b]
    "Given a pair of operations a and b, explains why b depends on a, in the
    form of a data structure. Returns `nil` if b does not depend on a.")

  (render-explanation
    [_ explanation a-name b-name]
    "Given an explanation from explain-pair-data, and short names for a and b,
    renders a string explaining why a depends on b."))

(defn explain-pair
  "Given a pair of operations, and short names for them, explain why b
  depends on a, as a string. `nil` indicates that b does not depend on a."
  [explainer a-name a b-name b]
  (when-let [ex (explain-pair-data explainer a b)]
    (render-explanation explainer ex a-name b-name)))

(defrecord CombinedExplainer [explainers]
  DataExplainer
  (explain-pair-data [this a b]
    ; This is SUCH an evil hack, but, uh, bear with me.
    ; When we render an explanation, we have no way to tell what explainer
    ; knows how to render it, and we don't want to make every explainer have to
    ; double-check whether they're rendering their own explanations, or if
    ; they're being passed someone else's. We could have a *wrapper type* here,
    ; but then we're introducing non-semantic data into the explanation which
    ; isn't necessary for users, makes it harder to read output, and makes it
    ; more difficult to test. Routing explanations to the explainers that
    ; generated them is just *plumbing*, and has no semantic place in our
    ; data model.
    ;
    ; Lord help me, I think this is the first time I've actually wanted to
    ; think about metadata outside of a macro or compiler context.
    (when-let [[i ex] (->> explainers
                           (map-indexed (fn [i e]
                                          [i (explain-pair-data e a b)]))
                           ; Find the first [i explanation] pair where ex is
                           ; present
                           (filter second)
                           first)]
      (vary-meta ex assoc this i)))

  (render-explanation [this ex a-name b-name]
    (let [i (get (meta ex) this)]
      (assert i (str "Not sure where explanation " (pr-str ex) " with meta " (pr-str (meta ex)) " came from!"))
      (render-explanation (nth explainers i) ex a-name b-name))))

(defn combine
  "Helpful in composing an analysis function for a checker out of multiple
  other analysis fns. For example, you might want a checker that looks for
  per-key monotonicity *and* real-time precedence---you could use:

  (checker (combine monotonic-keys real-time))"
  [& analyzers]
  (fn analyze [history]
    (loop [analyzers  analyzers
           anomalies  nil
           graph      (linear (digraph))
           explainers []]
      (if (seq analyzers)
        ; Use this analyzer on the history and merge its result map into our
        ; state.
        (let [analyzer (first analyzers)
              analysis (analyzer history)]
          (assert (and (:graph analysis) (:explainer analysis))
                  (str "Analyzer " (pr-str analyzer)
                       " did not return a map with a :graph and :analysis."
                       " Instead, we got " (pr-str analysis)))
          (recur (next analyzers)
                 (merge anomalies (:anomalies analysis))
                 (digraph-union graph (:graph analysis))
                 (conj explainers (:explainer analysis))))
        ; Done!
      {:anomalies anomalies
       :graph     (.forked graph)
       :explainer (CombinedExplainer. explainers)}))))

;; Monotonic keys!

(defrecord MonotonicKeyExplainer []
  DataExplainer
  (explain-pair-data [_ a b]
    (let [a (:value a)
          b (:value b)]
      ; Find keys in common
      (->> (keys a)
           (filter b)
           (reduce (fn [_ k]
                     (let [a (get a k)
                           b (get b k)]
                       (when (and a b (< a b))
                         (reduced {:type   :monotonic
                                   :key    k
                                   :value  a
                                   :value' b}))))
                   nil))))

  (render-explanation [_ {:keys [key value value']} a-name b-name]
    (str a-name " observed " (pr-str key) " = "
         (pr-str value) ", and " b-name
         " observed a higher value "
         (pr-str value'))))

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
                   (link-all-to-all g ops1 ops2 :monotonic-key))
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
    {:graph graph
     :explainer (MonotonicKeyExplainer.)}))

;; Processes

(defn process-order
  "Given a history and a process ID, constructs a partial order graph based on
  all operations that process performed."
  [history process]
  (->> history
       (filter (comp #{process} :process))
       (partition 2 1)
       (reduce (fn [g [op1 op2]] (link g op1 op2 :process))
               (.linear (DirectedGraph.)))
       forked))

(defrecord ProcessExplainer []
  DataExplainer
  (explain-pair-data [_ a b]
    (when (and (= (:process a) (:process b))
               (< (:index a) (:index b)))
      {:type      :process
       :process   (:process a)}))

  (render-explanation [_ {:keys [process]} a-name b-name]
    (str "process " process " executed " a-name " before " b-name)))

(defn process-graph
  "Analyses histories and relates operations performed sequentially by each
  process, such that every operation a process performs is ordered (but
  operations across different processes are not related)."
  [history]
  (let [oks (filter op/ok? history) ; TODO: order infos?
        graph (->> oks
                   (map :process)
                   distinct
                   (map (fn [p] (process-order oks p)))
                   (reduce digraph-union))]
    {:graph     graph
     :explainer (ProcessExplainer.)}))

; Realtime order

(defrecord RealtimeExplainer [pairs]
  DataExplainer
  (explain-pair-data [_ a' b']
    (let [b (get pairs b')]
      (when (< (:index a') (:index b))
        {:type :realtime
         :a'   a'
         :b    b})))

  (render-explanation [_ {:keys [a' b]} a'-name b'-name]
    (str a'-name " completed at index " (:index a') ","
         (when (and (:time a') (:time b))
           (let [dt (- (:time b) (:time a'))]
             ; Times are approximate--what matters is the serialization
             ; order. If we observe a zero or negative delta, just fudge it.
             (if (pos? dt)
               (str (format " %.3f" (util/nanos->secs (- (:time b) (:time a'))))
                    " seconds")
               ; Times are inaccurate
               " just")))
         " before the invocation of " b'-name
         ", at index " (:index b))))

(defn realtime-graph
  "Given a history, produces {:graph g :explainer e}, which encodes the
  real-time dependencies of transactions: a < b if a ends before b begins.

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
      (if-let [op (first history)]
        (case (:type op)
          ; A new operation begins! Link every completed op to this one's
          ; completion. Note that we generate edges here regardless of whether
          ; this op will fail or crash--we might not actually NEED edges to
          ; failures, but I don't think they'll hurt. We *do* need edges to
          ; crashed ops, because they may complete later on.
          :invoke (let [op' (get pairs op)
                        g   (reduce (fn [g ok]
                                      (link g ok op' :realtime))
                                    g
                                    oks)]
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
        {:graph     g
         :explainer (RealtimeExplainer. pairs)}))))

;; Graph search

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
  "Given a graph, and a starting collection of paths, constructs shells
  outwards from those paths: collections of longer and longer paths."
  [^DirectedGraph g starting-paths]
  ; The longest possible cycle is the entire graph, plus one.
  (take (inc (.size g))
        (iterate (comp prune-alternate-paths
                       (partial grow-paths g)
                       prune-longer-paths)
                 starting-paths)))

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

(defn find-cycle-starting-with
  "Some anomalies consist of a cycle with exactly, or at least, one edge of a
  particular type. This fuction works like find-cycle, but allows you to pass
  *two* graphs: an initial graph, which is used for the first step, and a
  remaining graph, which is used for later steps."
  [^IGraph initial-graph ^IGraph remaining-graph scc]
  (let [scc   (->bset scc)
        ig    (.select initial-graph scc)
        rg    (.select remaining-graph scc)
        ; Think about the structure of these two graphs
        ;
        ;  I        R
        ; ┌── d ┆
        ; ↓   ↑ ┆
        ; a ──┤ ┆
        ;     ↓ ┆
        ; b → c ┆ c → a
        ;
        ; In this graph, a cycle starting with I and completing with R can't
        ; include d, since there is no way for d to continue in R--it's not
        ; present in that set. Note that this is true even though [a d a] is a
        ; cycle--it's just a cycle purely in I, rather than a cycle starting in
        ; I and continuing in R, which is what we're looking for.
        ;
        ; Similarly, b can't be in a cycle, because there's no way to *return*
        ; to b from R, and we know that the final step in our cycle must be a
        ; transition from R -> I.
        ;
        ; In general, a cycle which has one edge from I must cross the boundary
        ; from I to R, and from R back to I again--otherwise it would not be a
        ; cycle. We therefore know that the first two vertices must be elements
        ; of both I *and* R. To encode this, we restrict I to only vertices
        ; present in R.
        ig (.select initial-graph (.vertices rg))]
    ; Start with a vertex in the initial graph
    (->> (.vertices ig)
         (keep (fn [start]
                 ; Expand this vertex out one step using the initial graph
                 (->> (grow-paths ig [[start]])
                      ; Then expand those paths into surrounding shells
                      (path-shells rg)
                      ; Flatten those into a list of paths
                      (mapcat identity)
                      (filter loop?)
                      first)))
         first)))

(defn find-cycle
  "Given a graph and a strongly connected component within it, finds a short
  cycle in that component."
  [^IGraph graph scc]
  (let [g             (.select graph (->bset scc))
        ; Just to speed up equality checks here, we'll construct an isomorphic
        ; graph with integers standing for our ops, find a cycle in THAT, then
        ; map back to our own space.
        [gn mapping]  (renumber-graph g)]
    (->> (.vertices gn)
         (keep (fn [start]
                 (when-let [cycle (->> (path-shells gn [[start]])
                                       (mapcat identity)
                                       (filter loop?)
                                       first)]
                     (mapv mapping cycle))))
         first)))

(defn explain-binding
  "Takes a seq of [name op] pairs, and constructs a string naming each op."
  [bindings]
  (str "Let:\n"
       (->> bindings
            (map (fn [[name txn]]
                   (str "  " name " = " (pr-str txn))))
            (str/join "\n"))))

(defn explain-cycle-pair-data
  "Takes a pair explainer and a pair of ops, and constructs a data structure
  explaining why a precedes b."
  [pair-explainer [a b]]
  (or (explain-pair-data pair-explainer a b)
      (throw (IllegalStateException.
               (str "Explainer\n"
                    (with-out-str (pprint pair-explainer))
                    "\nwas unable to explain the relationship"
                    " between " (pr-str a)
                    " and " (pr-str b))))))

(defn explain-cycle-ops
  "Takes an pair explainer, a binding, and a sequence of steps: each an
  explanation data structure of one pair. Produces a string explaining why they
  follow in that order."
  [pair-explainer binding steps]
  (let [explanations (map (fn [[a-name b-name] step]
                            (str a-name " < " b-name ", because "
                                 (render-explanation
                                   pair-explainer step a-name b-name)))
                          ; Take pairs of names from the binding
                          (partition 2 1 (cycle (map first binding)))
                          steps)
        prefix       "  - "]
    (->> explanations
         (map-indexed (fn [i ex]
                        (if (= i (dec (count explanations)))
                          (str prefix "However, " ex ": a contradiction!")
                          (str prefix ex ".\n"))))
         str/join)))

; This protocol gives us the ability to take a cycle of operations and produce
; a data structure describing that cycle, and to render that cycle explanation
; as a string.
(defprotocol CycleExplainer
  (explain-cycle
    [_ pair-explainer cycle]
    "Takes a cycle and constructs a data structure describing it.")

  (render-cycle-explanation
    [_ pair-explainer explanation]
    "Takes an explanation of a cycle and renders it to a string."))

(def cycle-explainer
  "This explainer just provides the step-by-step explanation of the
  relationships between pairs of operations."
  (reify CycleExplainer
    (explain-cycle [_ pair-explainer cycle]
      ; Take pairs of ops from the cycle, and construct an explanation for each.
      {:cycle cycle
       :steps (->> cycle
                   (partition 2 1)
                   (map (partial explain-cycle-pair-data pair-explainer)))})

    (render-cycle-explanation [_ pair-explainer {:keys [cycle steps]}]
      ; Number the transactions in the cycle
      (let [binding (->> (butlast cycle)
                         (map-indexed (fn [i op] [(str "T" (inc i)) op])))]
        ; Explain the binding, then each step.
        (str (explain-binding binding)
             "\n\nThen:\n"
             (explain-cycle-ops pair-explainer binding steps))))))

(defn explain-scc
  "Takes a graph, a cycle explainer, a pair explainer, and a strongly connected
  component (a collection of ops) in that graph. Using those graphs, constructs
  a string explaining the cycle."
  [graph cycle-explainer pair-explainer scc]
  (->> (find-cycle graph scc)
       (explain-cycle cycle-explainer pair-explainer)
       (render-cycle-explanation cycle-explainer pair-explainer)))

(defn check
  "Meat of the checker. Takes an analysis function and a history; returns an map
  of:

      {:graph     The computed graph
       :explainer The explainer for that graph
       :cycles    A list of cycles we found
       :sccs      A set of strongly connected components
       :anomalies Any anomalies we found during this analysis.}"
  [analyze-fn history]
  (let [history           (remove (comp #{:nemesis} :process) history)
        {:keys [anomalies explainer graph]} (analyze-fn history)
        sccs              (strongly-connected-components graph)
        cycles            (->> sccs
                               (sort-by (comp :index first))
                               (mapv (partial explain-scc graph
                                              cycle-explainer
                                              explainer)))
        ; It's almost certainly the case that something went wrong if we didn't
        ; infer *any* dependencies.
        anomalies (if (= 0 (.size graph))
                    (assoc anomalies :empty-transaction-graph true)
                    anomalies)]
    {:graph     graph
     :explainer explainer
     :sccs      sccs
     :cycles    cycles
     :anomalies anomalies}))

(defn write-cycles!
  "Writes cycles to a file. Opts:

  :subdirectory   What subdirectory to write to
  :filename       What to call the file"
  [test opts cycles]
  (when (and (seq cycles)
             ; These are purely here so we don't have to fill in test
             ; maps with names and times in our internal tests.
             (:name test)
             (:start-time test))
    (->> cycles
         (str/join "\n\n\n")
         (spit (store/path! test (:subdirectory opts)
                            (:filename opts "cycles.txt"))))))

(defn checker
  "Takes a function which takes a history and returns a [graph, explainer]
  pair, and returns a checker which uses those graphs to identify cyclic
  dependencies."
  [analyze-fn]
  (reify checker/Checker
    (check [this test history opts]
      (try+
        (let [{:keys [graph explainer sccs cycles]} (check analyze-fn history)]
          (write-cycles! test opts cycles)
          {:valid?     (empty? sccs)
           :scc-count  (count sccs)
           :cycles     cycles})
        ; The graph analysis might decide that a certain history isn't
        ; analyzable (perhaps because it violates expectations the analyzer
        ; needed to hold in order to infer dependencies), or it might discover
        ; violations that *can't* be encoded as a part of the dependency graph.
        ; If that happens, the analyzer can throw an exception with a :valid?
        ; key, and we'll simply return the ex-info map.
        (catch [:valid? false] e e)
        (catch [:valid? :unknown] e e)))))
