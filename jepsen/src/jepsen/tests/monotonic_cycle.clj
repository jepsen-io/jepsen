(ns jepsen.tests.monotonic-cycle
  "A checker which searches for incidents of read skew. Because each register
  is increment-only, we know that there should never exist a pair of reads r1
  and r2, such that for two registers x and y, where both registers are
  observed by both reads, x_r1 < x_r2 and y_r1 > y_r2.

  This problem is equivalent to cycle detection: we have a set of partial
  orders <x, <y, ..., each of which relates states based on whether x increases
  or not. We're trying to determine whether these orders(not ) are *compatible*.

  Imagine an order <x as a graph over states, and likewise for <y, <z, etc.
  Take the union of these graphs. If all these orders are compatible, there
  should be no cycles in this graph.

  To do this, we take each key k, and find all values for k. In general, the
  ordering relation <k is the transitive closure, but for cycle detection, we
  don't actually need the full closure--we'll restrict ourselves to k=1's
  successors being those with k=2 (or, if there are no k=2, use k=3, etc). This
  gives us a set of directed edges over states for k; we union the graphs for
  all k together to obtain a graph of all relationships.

  Next, we apply Tarjan's algorithm for strongly connected components, which is
  linear in edges + vertices (which is why we don't work with the full
  transitive closure of <k). The existence of any strongly connected components
  containing more than one vertex implies a cycle in the graph, and that cycle
  will be within that component.

  This isn't suuuper ideal... the connected component could, I guess, be fairly
  large, and then it'd be hard to prove where the cycle lies. But this feels
  like an OK start."
  (:require [jepsen.checker :as checker]
            [jepsen.util :as util]
            [knossos.op :as op]
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
  (->clj [x] x))

(defn forked
  "Bifurcan's analogue to (persistent! g)"
  [^DirectedGraph graph]
  (.forked graph))

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
  (->> history
       (mapcat (comp keys :value))
       distinct
       (map (fn [k] [[:key k] (monotonic-key-order k history)]))
       (into (sorted-map))))

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
      (let [h          (->> history
                            (filter op/ok?)
                            (filter #(= :read (:f %))))
            orders    (orders-fn h)
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
