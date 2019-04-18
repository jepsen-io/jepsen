(ns jepsen.tests.cycle
  "Tests based on detecting cycles between operations in a history.

  First, we define a bunch of ways you can compute a dependency graph over a
  history. Our graphs use completions (e.g. ok, info) for the canonical
  representation of an operation. For instance:

  `realtime-order` relates a->b if operation b begins after operation a
  completes successfully.

  `process-order` relates a->b if a and b are performed by the same process,
  and that process executes a before b.

  `monotonic-key-order` assumes op :values are maps of keys to observed values,
  like {:x 1 :y 2}. It relates a->b if b observed a higher value of some key
  than a did. For instance, {:x 1 :y 2} -> {:x 2 :y 2}, because :x is higher in
  the second op.

  `wr-order` assumes op :values are transactions like [[f k v] ...], and
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
                    [txn :as txn]
                    [util :as util]]
            [jepsen.txn.micro-op :as mop]
            [knossos [op :as op]
                     [history :refer [pair-index]]]
            [clojure.tools.logging :refer [info error warn]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [jepsen.generator :as gen]
            [fipp.edn :refer [pprint]])
  (:import (io.lacuna.bifurcan DirectedGraph
                               Graphs
                               ISet
                               IGraph
                               SortedMap)))

(set! *warn-on-reflection* true)

; Convert stuff back to Clojure data structures
(defprotocol ToClj
  (->clj [x]))

(extend-protocol ToClj
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
            (reduce (fn [^DirectedGraph graph succ]
                      (.link graph node succ))
                    g
                    succs))
          (.linear (DirectedGraph.))
          m))

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

(defn combine
  "Helpful in composing an order function for a checker out of multiple order
  fns. For example, you might want a checker that looks for per-key
  monotonicity *and* real-time precedence---you could use:

  (checker (combine monotonic-key-orders real-time))"
  [& order-fns]
  (fn [history]
    (->> order-fns
         (map (fn [order-fn] (order-fn history)))
         (reduce merge))))

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

(defn monotonic-key-orders
  "Constructs a map of value keys to orders on those keys, assuming each value
  monotonically increases."
  [history]
  (let [history (filter op/ok? history)]
    (->> history
         (mapcat (comp keys :value))
         distinct
         (map (fn [k] [[:key k] (monotonic-key-order k history)]))
         (into (sorted-map)))))

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

(defn process-orders
  "Constructs a map of processes to orders on those particular processes."
  [history]
  (let [oks (filter op/ok? history)]
    (->> oks
         (map :process)
         distinct
         (map (fn [p] [[:process p] (process-order oks p)]))
         (into (sorted-map)))))

(defn realtime-order
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
                        _ (info op :implied implied)
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
        {:realtime g}))))

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

(defn wr-orders
  "Given a history where ops are txns (e.g. [[:r :x 2] [:w :y 3]]), constructs
  an order over txns based on the external writes and reads of key k: any txn
  that reads value v must come after the txn that wrote v."
  [history]
  (let [ext-writes (ext-index txn/ext-writes  history)
        ext-reads  (ext-index txn/ext-reads   history)]
    ; Take all reads and relate them to prior writes.
    (reduce (fn [orders [k values->reads]]
              ; OK, we've got a map of values to ops that read those values
              (->> values->reads
                   (reduce (fn [k-graph [v reads]]
                             ; Find ops that set k=v
                             (let [writes (-> ext-writes (get k) (get v))]
                               ; There should be exactly one--if there's more
                               ; than one, we can't do this sort of cycle
                               ; analysis because there are multiple
                               ; alternative orders.
                               ;
                               ; Technically, it'd be legal to ignore these,
                               ; but I think it's likely the case that users
                               ; will want a big flashing warning if they mess
                               ; this up.
                               (assert (< (count writes) 2)
                                       (throw (IllegalArgumentException.
                                                (str "Key " (pr-str k)
                                                     " had value " (pr-str v)
                                                     " written by more than one op: "
                                                     (pr-str writes)))))
                               ; OK, this write needs to precede all reads
                               (link-to-all k-graph (first writes) reads)))
                             (DirectedGraph.))
                   ; Now we have a graph of ops related by K. Merge that into
                   ; our orders map!
                   (assoc orders [:key k])))
            {}
            ext-reads)))

(defn full-graph
  "Takes a map of names to orders, and computes the union of those orders as a
  Bifurcan DirectedGraph."
  [orders]
  (reduce digraph-union (vals orders)))

(defn explain-cycle
  "Takes a map of orders, and a cycle (a collection of ops) in that cycle.
  Using those orders, constructs a chain of [op1 relationship op2 relationship
  ... op1], illustrating how they form a cycle.

      [{:process 0 :value {:x 5}}
       [:process 0] {:process 0 :value {:x 3}}
       [:key :x]    {:process 1 :value {:x 4}}
       [:key :x]    {:process 0 :value {:x 5}}]"
  [orders cycle]
  ; TODO
  cycle)

(defn checker [orders-fn]
  (reify checker/Checker
    (check [this test history opts]
      (let [history   (remove (comp #{:nemesis} :process) history)
            orders    (orders-fn history)
            ; _         (info :orders (with-out-str (pprint orders)))
            graph     (full-graph orders)
            cycles    (strongly-connected-components graph)]
         {:valid? (empty? cycles)
          :cycles cycles}))))

(defn w-inc [ks]
  {:f :inc, :type :invoke, :value (vec ks)})

(defn r [ks]
  (let [v (->> ks
               (map (fn [k] [k nil]))
               (into {}))]
    {:f :read, :type :invoke, :value v}))

(defn workload
  "A package of a generator and checker. Options:

    :keys   A set of registers you're going to operate on. Allows us to generate
            monotonically increasing writes per key, and create reads for n keys.
    :read-n How many keys to read from at once. Default 2."
  [{:keys [keys read-n]}]
  {:checker (checker monotonic-key-orders)
   ;; FIXME
   :generator (gen/mix [w-inc r])})
