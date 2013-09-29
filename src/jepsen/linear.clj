(ns jepsen.linear
  "The big question: is a database linearizable?

  A more specific sub-question: Is a given sequence of
  operations executed on a database linearizable?

  This is, in essence, a search problem. *Find* a causal history which takes us
  from state A to state B. If no such history exists, we've found a bug in the
  database.

  We can't ask the database for the history directly, because a.) it doesn't
  know and b.) it's probably lying anyways. Instead we have to *infer* the
  history from:

    1. The operations we submitted to the database. The history we're looking
    for will be a k-permutation of those operations.

    2. Whether or not the DB acknowledged the write as successful. Every
    acknowledged write will be present in the history.

    3. The initial and final states of the database. These set the *boundary
    conditions* for our model; if the database and the model disagree about the
    initial or final state, given the same paths, we know the path wasn't the
    linearization we were looking for.

  The state space of paths is big. Fucking awful, truth be told. For N
  operations, there are 2^n possible *sets* of causal histories if none of them
  are known to have completed (only 1 if every operation completes). Each one
  of those sets of operations has n! possible interleavings, and realistically,
  pruning that search space via constraints on the times of commit points is
  tough.

  # What do we *have*?

  We have a known set of operations. We also have a model which, given an
  initial state and a history, can produce a final state.
 
  # Compute an index
 
  Given an initial state and a set of operations, generate every possible
  history and apply the model to that history to yield a final state. Then
  compute an *index* containing all possible tuples of
  
  [initial-state final-state operation-multiset]

  Then, given an initial and final state of the *database*, a set of operations
  *known* to have been applied to the database, and a set of *potential*
  operations which may or may not have been applied, verifying linearizability
  just means finding an element in the index such that the operation multiset
  is a *superset* of the known applied operations, and a *subset* of the
  potentially applied operations.

  # Efficiently traversing the index

  The initial and final states are easy to index; just use a hashmap. The
  super/subset constraints on potential operations are a little bit trickier,
  though. We know the theoretical bounds are O(c * |potential operations|).
  What data structures would get us there?

  Assume a total order on operations. Then every multiset of operations has a
  single representation obtained by just ordering the list.

  Imagine, then, a radix tree in which a path from the root to any leaf
  constitutes a multiset of the operations in a valid linearization. A multiset
  of operations allows a linearizable history if we can find a path from the
  root to the leaf which

  - Visits a node for every operation known to have completed
  - Does not visit any nodes which are known not to have been completed

  Testing linearizability then reduces to depth-first search of the tree. Keep
  a set of mandatory elements and possible elements. At each node, descend into
  every node for which you have a mandatory or possible element remaining (and
  remove that element from the corresponding set in the recursion. If you
  encounter a leaf node and the mandatory set is empty, terminate with true.
 
  We can do this efficiently because the tree and our operation multiset are
  both ordered.

  {:a {:b {:b {:c}
          {:c}}}}

  Valid multisets here are [:a :b :b :c] and [:a :b :c]. Compact nodes with
  single children to transform this into a radix tree."

  (:require [clojure.math.combinatorics :as combinatorics]))

{:op    :cas
 :v1    :foo
 :v2    :bar
 :start 12
 :end   15
 :ret   (:success :indeterminate)}

(defn linearization?
  "Is the given sequence of operations a valid linearization?
  
  A linearization requires that every operation appear to commit at a specific
  time between :start and :end. We always know the start time, because we
  initiated the operation. We *don't* always know the end time.
  
  We know an operation is *not* part of a linearization if its commit time is
  before any previous operation's commit time. Because we're dealing with fuzzy
  ranges, this is only true if the operation *ends* before any previous op
  begins. Essentially, we're computing non-intersecting ranges.
  
  If we don't know when the operation ended, it is always a valid
  linearization."
  [operations]
  (if (empty? operations)
    true
    (->> operations
         (reduce (fn [largest-prior-start-time op]
                   (if (and (:end op)
                            (< (:end op) largest-prior-start-time))
                     (reduced false)
                     (max largest-prior-start-time (:start op))))
                 (:start (first operations)))
         boolean)))

(defn linearizations
  "Given a sequence of operations, computes all possible linearizations of
  those operations."
  [operations]
  (->> operations
       combinatorics/permutations
       (filter linearization?)))

(defn random-op
  []
  (let [t1 (rand-int 10)]
    {:start t1
     :end   (when (< 0.5 (rand))
              (+ t1 (rand-int 2)))}))

(defn random-ops
  []
  (repeatedly random-op))
