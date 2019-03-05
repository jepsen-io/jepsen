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
            [knossos.op :as op]
            [clojure.tools.logging :refer [info error warn]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.pprint :refer [pprint]]
            [jepsen.util :as util]))

;; FIXME Stack overflow on n>10000 histories
;; Discussion of purely functional version.
;; https://stackoverflow.com/a/15912896
;;
;; Parallelized SCC detection algorithms
;; https://www.cs.vu.nl/~wanf/theses/matei.pdf
;; if it's O(n) but has a cap... maybe we remove the cap?
;; Isn't parallelization trying to speed up something that's already fast?
;; We're trying to increase the upper bound, not the processing speed
;; parallelization would by nature increase the size.
;; You'd have to stack share (Matei ?) to parallelize DFS though
(defn tarjan
  "Returns the strongly connected components of a graph specified by its nodes
  and a successor function (next) succs from node to nodes. An implementation of
  Tarjan's Strongly Connected Components.
  From: http://clj-me.cgrand.net/2013/03/18/tarjans-strongly-connected-components-algorithm/"
  [succs]
  (let [nodes (keys succs)]
    (letfn [(sc [state node]
              (if (contains? state node)
                ;; Skip
                state

                ;; Algorithm state is namespaced to avoid intersection with graph
                (let [stack (::stack state)
                      n     (count stack)
                      state (assoc state
                                   node n
                                   ::stack (conj stack node))
                      state (reduce (fn [state next]
                                      (let [state (sc state next)
                                            x     (min (or (state next) n)
                                                       (state node))]

                                        (assoc state node x)))
                                    state
                                    (succs node))]

                  (if (= n (state node))

                    ;; No link below us in the stack, assign to SCCs
                    (let [nodes (::stack state)
                          scc (set (take (- (count nodes) n) nodes))
                          ;; clear all stack lengths for these nodes since this SCC is done
                          state (reduce #(assoc %1 %2 nil) state scc)]

                      (assoc state
                             ::stack stack
                             ::sccs (conj (::sccs state) scc)))

                    ;; ???
                    state))))]
      (let [state  {::stack () ::sccs #{}}
            result (reduce sc state nodes)]
        (::sccs result)))))

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
  "Honestly not very proud of this one. Looks like something I would've written
  when I first started clojure. But i've been staring at this ns for about 7
  hours and i'm just gonna pour this one out and leave it for tomorrow FIXME"
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

{:x {#{0 1} true}, :y nil}

(defn w [v] {:f :write, :type :invoke, :value v})
(defn r [] {:f :read, :type :invoke})

(defn workload
  []
  {:checker (checker)
   ;; TODO Gen
   :generator []})
