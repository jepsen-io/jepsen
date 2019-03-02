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
            [clojure.core.reducers :as r]
            [clojure.set :as set]))

(defn tarjan
  "Returns the strongly connected components of a graph specified by its nodes
  and a successor function (next) succs from node to nodes. An implementation of
  Tarjan's Strongly Connected Components.
  From: http://clj-me.cgrand.net/2013/03/18/tarjans-strongly-connected-components-algorithm/"
  [nodes succs]
  ;; Env is a map from nodes to stack length or nil, nil means the node is
  ;; known to belong to another SCC (Strongly Connected Component) :stack for
  ;; the current stack and :sccs for the current set of SCCs.
  (letfn [(sc [env node]
            (if (contains? env node)
              env
              (let [stack (:stack env)
                    n     (count stack)
                    env   (assoc env node n :stack (conj stack node))
                    env   (reduce (fn [env next]
                                    (let [env (sc env next)]
                                      (assoc env node (min (or (env next) n) (env node)))))
                                  env (succs node))]
                ;; No link below us in the stack, assign to SCCs
                (if (= n (env node))
                  (let [nodes (:stack env)
                        scc (set (take (- (count nodes) n) nodes))
                        ;; clear all stack lengths for these nodes since this SCC is done
                        env (reduce #(assoc %1 %2 nil) env scc)]
                    (assoc env :stack stack :sccs (conj (:sccs env) scc)))
                  env))))]
    (let [state  {:stack '() :sccs #{}}
          result (reduce sc state nodes)]
      (:sccs result))))

(merge-with clojure.set/union {} {0 #{nil}} {1 #{0}})

(defn graph
  "Takes a history of reads over a single register and returns a graph of the
  states that the register advanced through.
  FIXME Stack overflow on n>10000 histories
  FIXME Handle multiple registers and transactions of reads"
  [history]
  (loop [graph {}
         [op & more :as history] history
         last nil]
    (let [val  (:value op)
          prev (if last
                 {last #{val}}
                 {})
          next {val #{}}
          g'   (merge-with set/union graph prev next)]
      (if more
        (recur g' more val)
        g'))))

;; TODO Can we improve this error output?
(defn errors
  "Takes a set of component-sets from tarjan's results, identifying if any
  components are strongly connected (more than 1 element per set)."
  [components]
  (let [<=2 (fn [set]
              {set (<= 2 (count set))})]
    (->> components
         (map <=2)
         (reduce merge))))

(defn checker []
  (reify checker/Checker
    (check [this test history opts]
      (let [h          (->> history
                            (filter op/ok?)
                            (filter #(= :read (:f %))))
            g          (graph h)
            components (tarjan (keys g) g)
            errors     (errors components)]
        {:valid? (not-any? true? (vals errors))
         :errors errors}))))

(defn w [v] {:f :write, :type :invoke, :value v})
(defn r [v] {:f :read,  :type :invoke})

(defn workload
  []
  {:checker (checker)
   ;; TODO Gen
   :generator []})
