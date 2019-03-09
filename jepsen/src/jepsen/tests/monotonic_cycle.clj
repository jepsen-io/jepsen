(ns jepsen.tests.monotonic-cycle
  "A checker which searches for incidents of read skew. Because each register
  is increment-only, we know that there should never exist a pair of reads r1
  and r2, such that for two registers x and y, where both registers are
  observed by both reads, x_r1 < x_r2 and y_r1 > y_r2.

  This problem is equivalent to cycle detection: we have a set of partial
  orders <x, <y, ..., each of which relates states based on whether x increases
  or not. We're trying to determine whether these orders are *compatible*.

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
            [jepsen.generator :as gen]))

(defn tarjan
  "Returns the strongly connected components of a graph specified by its
  nodes (ints) and a successor function (succs node) from node to nodes.
  A iterative verison of Tarjan's Strongly Connected Components."
  [succs]
  (let [nodes (keys succs)
        ;; Ensure there's a table index for every value we could find
        table-size (->> nodes (apply max) inc)
        result (loop [index      0
                      indices    (int-array table-size)
                      visited    (make-array Boolean/TYPE table-size)
                      iterator   (.iterator nodes)
                      iter-stack '()
                      min-stack  '()
                      stack      '()
                      prev-node  nil
                      sccs       #{}]
                 ;; Depth-first search
                 (if-let [node (when (.hasNext iterator)
                                 (.next iterator))]
                   ;; Has the  node been visited?
                   (if-not (aget visited node)
                     ;; New node! Go down a level
                     (let [_          (aset indices node index)
                           _          (aset visited node true)
                           stack      (conj stack node)

                           m          (aget indices node)
                           min-stack  (conj min-stack m)
                           iter-stack (conj iter-stack iterator)
                           iterator   (.iterator (succs node))
                           index      (inc index)]
                       (recur index indices visited iterator iter-stack min-stack stack node sccs))

                     ;; We've seen this before, update the min stack
                     (let [min-stack (if (< 0 (count min-stack)) ; Ensure we have a recent minimum
                                       (let [;; We do, let's grab it
                                             m         (peek min-stack)
                                             min-stack (pop min-stack)
                                             ;; Compare the recent lowest to node's current index
                                             index (aget indices node)
                                             ;; Find the smaller of the node's index or last min
                                             low   (min index m)]
                                         (conj min-stack low))
                                       min-stack)]
                       (recur index indices visited iterator iter-stack min-stack stack node sccs)))

                   ;; Done checking edges this set of edges,
                   ;; look at node and see if component
                   (if (zero? (count iter-stack))
                     sccs
                     (let [iterator   (peek iter-stack)
                           iter-stack (pop iter-stack)

                           ;; Pop off the latest min
                           m          (peek min-stack)
                           min-stack  (pop min-stack)

                           [stack scc] (if (< m (aget indices prev-node))
                                         (do
                                           (aset indices prev-node m)
                                           [stack nil])
                                         (loop [w     nil
                                                stack stack
                                                scc   #{}]
                                           (let [w (or (peek stack) -1)]
                                             (if (<= prev-node w)
                                               (let [stack (try
                                                             (pop stack)
                                                             (catch java.lang.IllegalStateException _
                                                               '()))
                                                     scc   (conj scc w)]
                                                 (aset indices w (count nodes))
                                                 (recur w stack scc))
                                               [stack scc]))))

                           ;; SCC good to go sir
                           sccs (if scc
                                  (conj sccs scc)
                                  sccs)

                           ;; Update head of min-stack
                           min-stack (if (< 0 (count min-stack))
                                       (let [m         (peek min-stack)
                                             min-stack (pop min-stack)

                                             i' (aget indices prev-node)
                                             ;; Find the smaller of the node's index or last min
                                             low       (min i' m)
                                             min-stack (conj min-stack low)]
                                         min-stack))]
                       (recur index indices visited iterator
                              iter-stack min-stack stack prev-node sccs)))))]
    result))

(defn apply-pair
  "Takes a map of registers to their last-seen values, a map of registers to
  their graphs, and a tuple containing a register and its new value. Returns
  the graph-map with the op applied to the appropriate register's graph."
  [last graph [k v]]
  (let [last-v (last k)
        node {v #{}}
        edge (if last-v
               {last-v #{v}}
               {})
        g (merge-with set/union (graph k) node edge)]
    (assoc-in graph [k] g)))

(defn graph
  "Takes a history of reads over a single register and returns a graph of the
  states that the register advanced through."
  [history]
  (loop [graph {}
         last  {}
         ;; This will probably have to be a map of keys to last-seen values
         [op & more] history]
    (let [val   (:value op)
          ;; Single register get a key called :register
          val   (if-not (map? val)
                  {:register val}
                  val)
          pairs (concat val)

          ;; Remove values that have not changed
          new-pairs (set/difference (set pairs)
                                    (set last))

          ;; Apply new nodes and edges to the graph
          g' (reduce (partial apply-pair last)
                     graph
                     new-pairs)]
      (if more
        (recur g' (merge last val) more)
        g'))))

(defn errors
  "Takes a set of component-sets from tarjan's results, identifying if any
  components are strongly connected (more than 1 element per set)."
  [components]
  (let [<=2 (fn [set]
              {set (<= 2 (count set))})
        error? (fn [m] (some true? (vals m)))]
    (->> components
         (map <=2)
         (filter error?)
         (reduce merge))))

(defn valid?
  "Takes a map of registers to their analyzed components. Returns true if
  none are invalid. Also auto-validates any single-register history."
  [errors]
  (if-not (:register errors)
    (->> errors
         (map second)
         (map vals)
         (apply concat)
         (some true?)
         not)
    true))

(defn checker []
  (reify checker/Checker
    (check [this test history opts]
      (let [h          (->> history
                            (filter op/ok?)
                            (filter #(= :read (:f %))))
            g          (graph h)
            components (util/map-vals tarjan g)
            errors     (util/map-vals errors components)]

        ;; Auto-validate single-key histories
        {:valid? (valid? errors)
         :errors errors}))))

(defn w [k v]
  {:f :write, :type :invoke, :value {k v}})

(defn r [keys]
  (let [v (->> keys
               (map (fn [k] [k nil]))
               (into {}))]
    {:f :read, :type :invoke, :value v}))


(defn workload
  "A package of a generator and checker. Options:

    :keys   A set of registers you're going to operate on. Allows us to generate
            monotonically increasing writes per key, and create reads for n keys.
    :read-n How many keys to read from at once. Default 2."
  [{:keys [keys read-n]}]
  {:checker (checker)
   ;; FIXME
   :generator (gen/mix [w r])})
