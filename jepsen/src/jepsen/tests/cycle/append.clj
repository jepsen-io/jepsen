(ns jepsen.tests.cycle.append
  "Detects cycles in histories where operations are transactions over named
  lists lists, and operations are either appends or reads. Used with
  jepsen.tests.cycle."
  (:require [jepsen [checker :as checker]
                    [generator :as gen]
                    [txn :as txn :refer [reduce-mops]]
                    [util :as util]]
            [jepsen.tests.cycle :as cycle]
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
(defn verify-mop-types
  "Takes a history where operation values are transactions. Verifies that the
  history contains only reads [:r k v] and appends [:append k v]. Returns nil if the history conforms, or throws an error object otherwise."
  [history]
  (let [bad (remove (fn [op] (every? #{:r :append} (map mop/f (:value op))))
                    history)]
    (when (seq bad)
      (throw+ {:valid?    :unknown
               :type      :unexpected-txn-micro-op-types
               :message   "history contained operations other than appends or reads"
               :examples  (take 8 bad)}))))

(defn verify-unique-appends
  "Takes a history of txns made up of appends and reads, and checks to make
  sure that every invoke appending a value to a key chose a unique value."
  [history]
  (reduce-mops (fn [written op [f k v]]
                 (if (and (op/invoke? op) (= :append f))
                   (let [writes-of-k (written k #{})]
                     (if (contains? writes-of-k v)
                       (throw+ {:valid?   :unknown
                                :type     :duplicate-appends
                                :op       op
                                :key      k
                                :value    v
                                :message  (str "value " v " appended to key " k
                                               " multiple times!")})
                       (assoc written k (conj writes-of-k v))))
                   ; Something other than an invoke append, whatever
                   written))
               {}
               history))

(defn prefix?
  "Given two sequences, returns true iff A is a prefix of B."
  [a b]
  (and (<= (count a) (count b))
       (loop [a a
              b b]
         (cond (nil? (seq a))           true
               (= (first a) (first b))  (recur (next a) (next b))
               true                     false))))

(defn values-from-single-appends
  "As a special case of sorted-values, if we have a key which only has a single
  append, we don't need a read: we can infer the sorted values it took on were
  simply [], [x]."
  [history]
  ; Build a map of keys to appended elements.
  (->> history
       (reduce-mops (fn [appends op [f k v]]
                      (cond ; If it's not an append, we don't care
                            (not= :append f) appends

                            ; There's already an append!
                            (contains? appends k)
                            (assoc appends k ::too-many)

                            ; No known appends; record
                            true
                            (assoc appends k v)))
                    {})
       (keep (fn [[k v]]
               (when (not= ::too-many v)
                 [k #{[v]}])))
       (into {})))

(defn sorted-values
  "Takes a history where operation values are transactions, and every micro-op
  in a transaction is an append or a read. Computes a map of keys to all
  distinct observed values for that key, ordered by length.

  As a special case, if we have a key which only has a single append, we don't
  need a read: we can infer the sorted values it took on were simply [], [x]."
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
               (values-from-single-appends history))
       ; And sort
       (util/map-vals (partial sort-by count))))

(defn verify-total-order
  "Takes a map of keys to observed values (e.g. from
  `sorted-values`, and verifies that for each key, the values
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
      (throw+ {:valid?  false
               :type    :no-total-state-order
               :message "observed mutually incompatible orders of appends"
               :errors  errors}))))

(defn verify-no-dups
  "Given a history, checks to make sure that no read contains *duplicate*
  copies of the same appended element. Since we only append values once, we
  should never see them more than that--and if we do, it's really gonna mess up
  our whole \"total order\" thing!"
  [history]
  (reduce-mops (fn [_ op [f k v]]
                 (when (= f :r)
                   (let [dups (->> (frequencies v)
                                   (filter (comp (partial < 1) val))
                                   (into (sorted-map)))]
                     (when (seq dups)
                       (throw+ {:valid?   false
                                :type     :duplicate-elements
                                :message  "Observed multiple copies of a value which was only inserted once"
                                :op         op
                                :key        k
                                :value      v
                                :duplicates dups})))))
               nil
               history))

(defn append-index
  "Takes a map of keys to observed values (e.g. from
  sorted-values), and builds a map of keys to indexes on
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

(defn write-index
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

(defn read-index
  "Takes a history restricted to oks and infos, and constructs a map of keys to
  append values to the operations which observed the state generated by the
  append of k. The special append value ::init generates the initial (nil)
  state.

  Note that reads of `nil` by :info ops don't result in an entry in this index,
  because those `nil`s denote the default read value, NOT that we actually
  observed `nil`."
  [history]
  (reduce-mops (fn [index op [f k v]]
                 (if (and (= :r f)
                          (not (and (= :info (:type op))
                                    (= :r f)
                                    (= nil v))))
                   (update-in index [k (or (peek v) ::init)] conj op)
                   index))
               {}
               history))

(defrecord Explainer [append-index write-index read-index]
  cycle/Explainer
  (explain-pair [_ a-name a b-name b]
    (->> (:value b)
         (keep (fn [[f k v]]
                 (case f
                   :r (when (seq v)
                        ; What wrote the last thing we saw?
                        (let [last-e (peek v)
                              append-op (-> write-index (get k) (get last-e))]
                          (when (= a append-op)
                            (str b-name " observed " a-name
                                 "'s append of " (pr-str last-e)
                                 " to key " (pr-str k)))))
                   :append
                   (when-let [index (get-in append-index [k :indices v])]
                     (let [; And what value was appended immediately before us
                           ; in version order?
                           prev-v (if (pos? index)
                                    (get-in append-index [k :values (dec index)])
                                    ::init)

                           ; What op wrote that value?
                           prev-append (when (pos? index)
                                         (get-in write-index [k prev-v]))

                           ; What ops read that value?
                           prev-reads (get-in read-index [k prev-v])]
                         (cond (= a prev-append)
                               (str b-name " appended " (pr-str v) " after "
                                    a-name " appended " (pr-str prev-v)
                                    " to " (pr-str k))

                               (some #{a} prev-reads)
                               (if (= ::init prev-v)
                                 (str a-name
                                      " observed the initial (nil) state of "
                                      (pr-str k) ", which " b-name
                                      " created by appending " (pr-str v))
                                 (str a-name " did not observe "
                                      b-name "'s append of " (pr-str v)
                                      " to " (pr-str k)))))))))
         first)))

(defn graph
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
   (verify-mop-types history)
   ; And that every append is unique
   (verify-unique-appends history)
   ; And that reads never observe duplicates
   (verify-no-dups history)

   ; We only care about ok and info ops; invocations don't matter, and failed
   ; ops can't contribute to the state.
   (let [history       (filter (comp #{:ok :info} :type) history)
         sorted-values (sorted-values history)]
     ;(prn :sorted-values sorted-values)
     ; Make sure we can actually compute a version order for each key
     (verify-total-order sorted-values)
     ; Compute indices
     (let [append-index (append-index sorted-values)
           write-index  (write-index history)
           read-index   (read-index history)
           ; And build our graph
           ;_     (prn :append-index append-index)
           ;_     (prn :write-index  write-index)
           ;_     (prn :read-index   read-index)
           graph (graph history append-index write-index read-index)]
       [graph
        (Explainer. append-index write-index read-index)])))
  ; Actually build the DirectedGraph
  ([history append-index write-index read-index]
   (reduce
     (fn op [g op]
       (reduce
         (fn mop [g [f k v]]
           (case f
             ; OK, we've got a read. What op appended the last thing we saw?
             :r (if (seq v)
                  (let [append-op (-> write-index (get k) (get (peek v)))]
                    (assert append-op)
                    (if (= op append-op)
                      ; We appended this particular value, and that append will
                      ; encode the w-w dependency we need for this read.
                      g
                      ; We depend on the other op that appended this.
                      (cycle/link g append-op op)))
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

                     ; What op wrote that value?
                     prev-append (when (pos? index)
                                   (get-in write-index [k prev-v]))

                     ; Link that writer to us
                     g (if (and (pos? index)           ; (if we weren't first)
                                (not= prev-append op)) ; (and it wasn't us)
                         (cycle/link g prev-append op)
                         g)

                     ; And what ops read that previous value?
                     prev-reads (get-in read-index [k prev-v])

                     ; Link them all to us
                     g (reduce (fn link-read [g r]
                                 (if (not= r op)
                                   (cycle/link g r op)
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
