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

(defn op-mops
  "A lazy sequence of all [op mop] pairs from a history."
  [history]
  (mapcat (fn [op] (map (fn [mop] [op mop]) (:value op))) history))

(defn verify-mop-types
  "Takes a history where operation values are transactions. Verifies that the
  history contains only reads [:r k v] and appends [:append k v]. Returns nil
  if the history conforms, or throws an error object otherwise." [history]
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

(defn g1a-cases
  "G1a, or aborted read, is an anomaly where a transaction reads data from an
  aborted transaction. For us, an aborted transaction is one that we know
  failed. Info transactions may abort, but if they do, the only way for us to
  TELL they aborted is by observing their writes, and if we observe their
  writes, we can't conclude they aborted, sooooo...

  This function takes a history (which should include :fail events!), and
  produces a sequence of error objects, each representing an operation which
  read state written by a failed transaction."
  [history]
  ; Build a map of keys to maps of failed elements to the ops that appended
  ; them.
  (let [failed (reduce-mops (fn index [failed op [f k v :as mop]]
                              (if (and (op/fail? op)
                                       (= :append f))
                                (assoc-in failed [k v] op)
                                failed))
                            {}
                            history)]
    ; Look for ok ops with a read mop of a failed append
    (->> history
         (filter op/ok?)
         op-mops
         (mapcat (fn [[op [f k v :as mop]]]
                   (when (= :r f)
                     (keep (fn [e]
                             (when-let [writer (get-in failed [k e])]
                               {:op        op
                                :mop       mop
                                :writer    writer
                                :element   e}))
                           v)))))))

(defn g1b-cases
  "G1b, or intermediate read, is an anomaly where a transaction T2 reads a
  state for key k that was written by another transaction, T1, that was not
  T1's final update to k.

  This function takes a history (which should include :fail events!), and
  produces a sequence of error objects, each representing a read of an
  intermediate state."
  [history]
  ; Build a map of keys to maps of intermediate elements to the ops that wrote
  ; them
  (let [im (reduce (fn [im op]
                     ; Find intermediate appends for this particular txn by
                     ; producing two maps: intermediate keys to elements, and
                     ; final keys to elements in this txn. We shift elements
                     ; from final to intermediate when they're overwritten.
                     (first
                       (reduce (fn [[im final :as state] [f k v]]
                                 (if (= :append f)
                                   (if-let [e (final k)]
                                     ; We have a previous write of k
                                     [(assoc-in im [k e] op)
                                      (assoc final k v)]
                                     ; No previous write
                                     [im (assoc final k v)])
                                   ; Something other than an append
                                   state))
                               [im {}]
                               (:value op))))
                   {}
                   history)]
    ; Look for ok ops with a read mop of an intermediate append
    (->> history
         (filter op/ok?)
         op-mops
         (keep (fn [[op [f k v :as mop]]]
                 (when (= :r f)
                   ; We've got an illegal read if our last element came from an
                   ; intermediate append.
                   (when-let [writer (get-in im [k (peek v)])]
                     ; Internal reads are OK!
                     (when (not= op writer)
                       {:op       op
                        :mop      mop
                        :writer   writer
                        :element  (peek v)}))))))))

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
                           (if (and (= :r f) (seq v))
                             ; Good, this is a read of something other than the
                             ; initial state
                             (-> states
                                 (get k #{})
                                 (conj v)
                                 (->> (assoc states k)))
                             ; Something else!
                             states))
                         states
                         (:value op)))
               {})
       ; If we can't infer anything from reads, see if we can use a single
       ; append operation to infer a value.
       (merge (values-from-single-appends history))
       ; And sort
       (util/map-vals (partial sort-by count))))

(defn incompatible-orders
  "Takes a map of keys to sorted observed values and verifies that for each key
  the values read are consistent with a total order of appends. For instance,
  these values are consistent:

     {:x [[1] [1 2 3]]}

  But these two are not:

     {:x [[1 2] [1 3 2]]}

  ... because the first is not a prefix of the second. Returns a sequence of
  anomaly maps, nil if none are present."
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
    (seq errors)))

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
  (when-let [errors (incompatible-orders sorted-values)]
    (throw+ {:valid?  false
             :type    :no-total-state-order
             :message "observed mutually incompatible orders of appends"
             :errors  errors})))

(defn duplicates
  "Given a history, finds operations which have duplicate copies of the same
  appended element. Since we only append values once, we should never see them
  more than that--and if we do, it could mess up our whole \"total order\"
  thing!"
  [history]
  (->> history
       op-mops
       (keep (fn [[op [f k v :as mop]]]
                 (when (= f :r)
                   (let [dups (->> (frequencies v)
                                   (filter (comp (partial < 1) val))
                                   (into (sorted-map)))]
                     (when (seq dups)
                       {:op         op
                        :mop        mop
                        :duplicates dups})))))
       seq))

(defn merge-orders
  "Takes two potentially incompatible read orders (sequences of elements), and
  computes a total order which is consistent with both of them: where there are
  conflicts, we drop those elements.

  First, we remove duplicates; an order shouldn't have them at all. Yes, this
  means we fail to compute some dependencies.

  In general, the differences between orders fall into some cases:

  1. One empty

      _
      1 2 3 4 5

     We simply pick the non-empty order.

  2. Same first element

     2 x y
     2 z

     Our order is [2] followed by the merged result of [x y] and [z].

  3. Different first elements followed by a common element

     3 y
     2 3

    We drop the smaller element and recur with [3 y] [3]. This isn't... exactly
  symmetric; we prefer longer and higher elements for tail-end conflicts, but I
  think that's still a justifiable choice. After all, we DID read both values,
  and it's sensible to compute a dependency based on any read. Might as well
  pick longer ones.

  Later, we should change the whole structure of append indexes to admit
  multiple prior txns rather than just one, and get rid of this."
  ([as bs]
   (merge-orders [] (distinct as) (distinct bs)))
  ([merged as bs]
   (cond (empty? as) (into merged bs)
         (empty? bs) (into merged as)

         (= (first as) (first bs))
         (recur (conj merged (first as)) (next as) (next bs))

         (< (first as) (first bs)) (recur merged (next as) bs)
         true                      (recur merged as (next bs)))))

(defn append-index
  "Takes a map of keys to observed values (e.g. from sorted-values), and builds
  a bidirectional index: a map of keys to indexes on those keys, where each
  index is a map relating appended values on that key to the order in which
  they were effectively appended by the database.

    {:x {:indices {v0 0, v1 1, v2 2}
         :values  [v0 v1 v2]}}

  We merge all observed orders on a key using merge-orders."
  [sorted-values]
  (util/map-vals (fn [values]
                   ; The last value will be the longest, and since every other
                   ; is a prefix, it includes all the information we need.
                   (let [vs (reduce merge-orders [] values)]
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

(defn wr-mop-dep
  "What (other) operation wrote the value just before this read mop?"
  [write-index op [f k v]]
  (when (seq v)
    ; It may be the case that a failed operation appended this--that'd be
    ; picked up by the G1a checker.
    (when-let [append-op (-> write-index (get k) (get (peek v)))]
      ; If we wrote this value, there's no external dep here.
      (when-not (= op append-op)
        append-op))))

(defn previously-appended-element
  "Given an append mop, finds the element that was appended immediately prior
  to this append. Returns ::init if this was the first append."
  [append-index write-index op [f k v]]
  ; We may not know what version this append was--for instance, if we never
  ; read a state reflecting this append.
  (when-let [index (get-in append-index [k :indices v])]
    ; What value was appended immediately before us in version order?
    (if (pos? index)
      (get-in append-index [k :values (dec index)])
      ::init)))

(defn ww-mop-dep
  "What (other) operation wrote the value just before this write mop?"
  [append-index write-index op [f k v :as mop]]
  (when-let [prev-e (previously-appended-element
                      append-index write-index op mop)]
      ; If we read the initial state, no writer precedes us
      (when (not= ::init prev-e)
        ; What op wrote that value?
        (let [writer (get-in write-index [k prev-e])]
          ; If we wrote it, there's no dependency here
          (when (not= op writer)
            writer)))))

(defn rw-mop-deps
  "The set of (other) operations which read the value just before this write
  mop."
  [append-index write-index read-index op [f k v :as mop]]
  (if-let [prev-e (previously-appended-element
                      append-index write-index op mop)]
    ; Find all ops that read the previous value, except us
    (-> (get-in read-index [k prev-e])
        set
        (disj op))
    ; Dunno what was appended before us
    #{}))

(defn mop-deps
  "A set of dependencies for a mop in an op."
  [append-index write-index read-index op [f :as mop]]
  (case f
    :append (let [deps (rw-mop-deps append-index write-index read-index op mop)
                  deps (if-let [d (ww-mop-dep append-index write-index op mop)]
                         (conj deps d)
                         deps)]
              deps)
    :r      (when-let [d (wr-mop-dep write-index op mop)]
              #{d})))

(defn op-deps
  "All dependencies for an op."
  [append-index write-index read-index op]
  (->> (:value op)
       (map (partial mop-deps append-index write-index read-index op))
       (reduce set/union #{})))

(defn preprocess
  "Before we do any graph computation, we need to preprocess the history,
  making sure it's well-formed. We return a map of:

  {:history       The history restricted to :ok and :info ops
   :append-index  An append index
   :write-index   A write index
   :read-index    A read index}"
  [history]
   ; Make sure there are only appends and reads
   (verify-mop-types history)
   ; And that every append is unique
   (verify-unique-appends history)

   ; We only care about ok and info ops; invocations don't matter, and failed
   ; ops can't contribute to the state.
   (let [history       (filter (comp #{:ok :info} :type) history)
         sorted-values (sorted-values history)]
     ; Compute indices
     (let [append-index (append-index sorted-values)
           write-index  (write-index history)
           read-index   (read-index history)]
       {:history      history
        :append-index append-index
        :write-index  write-index
        :read-index   read-index})))

(defrecord WWExplainer [append-index write-index read-index]
  cycle/DataExplainer
  (explain-pair-data [_ a b]
    (->> (:value b)
         (keep (fn [[f k v :as mop]]
                 (when (= f :append)
                   ; We only care about write-write cycles
                   (when-let [prev-v (previously-appended-element
                                       append-index write-index b mop)]
                     ; What op wrote that value?
                     (when-let [dep (ww-mop-dep append-index write-index b mop)]
                       (when (= a dep)
                         {:type     :ww
                          :key      k
                          :value    prev-v
                          :value'   v}))))))
         first))

  (render-explanation [_ {:keys [key value value'] :as m} a-name b-name]
    (str b-name " appended " (pr-str value') " after "
         a-name " appended " (pr-str value)
         " to " (pr-str key))))

(defn ww-graph
  "Analyzes write-write dependencies."
  [history]
  (let [{:keys [history append-index write-index read-index]} (preprocess
                                                                history)]
    [(cycle/forked
       (reduce-mops (fn [g op [f :as mop]]
                      ; Only appends have dependencies, cuz we're interested in
                      ; ww cycles.
                      (if (not= f :append)
                        g
                        (if-let [dep (ww-mop-dep
                                       append-index write-index op mop)]
                          (cycle/link g dep op :ww)
                          g)))
                    (.linear (DirectedGraph.))
                    history))
     (WWExplainer. append-index write-index read-index)]))

(defrecord WRExplainer [append-index write-index read-index]
  cycle/DataExplainer
  (explain-pair-data [_ a b]
    (->> (:value b)
         (keep (fn [[f k v :as mop]]
                 (when (= f :r)
                   (when-let [writer (wr-mop-dep write-index b mop)]
                     (when (= writer a)
                       {:type  :wr
                        :key   k
                        :value (peek v)})))))
         first))

  (render-explanation [_ {:keys [key value]} a-name b-name]
    (str b-name " observed " a-name
         "'s append of " (pr-str value)
         " to key " (pr-str key))))

(defn wr-graph
  "Analyzes write-read dependencies."
  [history]
  (let [{:keys [history append-index write-index read-index]} (preprocess
                                                                history)]
    [(cycle/forked
       (reduce-mops (fn [g op [f :as mop]]
                      (if (not= f :r)
                        g
                        ; Figure out what write we overwrote
                        (if-let [dep (wr-mop-dep write-index op mop)]
                          (cycle/link g dep op :wr)
                          ; No dep
                          g)))
                    (.linear (DirectedGraph.))
                    history))
     (WRExplainer. append-index write-index read-index)]))

(defrecord RWExplainer [append-index write-index read-index]
  cycle/DataExplainer
  (explain-pair-data [_ a b]
    (->> (:value b)
         (keep (fn [[f k v :as mop]]
                 (when (= f :append)
                   (when-let [readers (rw-mop-deps append-index write-index
                                                   read-index b mop)]
                     (when (some #{a} readers)
                       (let [prev-v (previously-appended-element
                                      append-index write-index b mop)]
                         {:type   :rw
                          :key    k
                          :value  prev-v
                          :value' v}))))))
         first))

  (render-explanation [_ {:keys [key value value']} a-name b-name]
    (if (= ::init value)
      (str a-name
           " observed the initial (nil) state of "
           (pr-str key) ", which " b-name
           " created by appending " (pr-str value'))
      (str a-name " did not observe "
           b-name "'s append of " (pr-str value')
           " to " (pr-str key)))))

(defn rw-graph
  "Analyzes read-write anti-dependencies."
  [history]
  (let [{:keys [history append-index write-index read-index]} (preprocess
                                                                history)]
    [(cycle/forked
       (reduce-mops (fn [g op [f :as mop]]
                      (if (= f :append)
                        ; Who read the state just before we wrote?
                        (if-let [deps (rw-mop-deps append-index write-index
                                                   read-index op mop)]
                          (cycle/link-all-to g deps op :rw)
                          g)
                        g))
                    (cycle/linear (cycle/directed-graph))
                    history))
     (RWExplainer. append-index write-index read-index)]))

(defn g1c-graph
  "Per Adya, Liskov, & O'Neil, phenomenon G1C encompasses any cycle made up of
  direct dependency edges (but does not include anti-dependencies)."
  [history]
  ((cycle/combine ww-graph wr-graph) history))

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
  [history]
  ((cycle/combine ww-graph wr-graph rw-graph) history))

(def cycle-explainer
  ; We categorize cycles based on their dependency edges
  (reify cycle/CycleExplainer
    (explain-cycle [_ pair-explainer cycle]
      (let [ex (cycle/explain-cycle cycle/cycle-explainer pair-explainer cycle)

            ; What types of relationships are involved here?
            type-freqs (frequencies (map :type (:steps ex)))
            ww (:ww type-freqs 0)
            wr (:wr type-freqs 0)
            rw (:rw type-freqs 0)]
        ; Tag the cycle with a type based on the edges involved. Note that we
        ; might have edges from, say, real-time or process orders, so we try to
        ; be permissive.
        (assoc ex :type (cond (< 1 rw) :G2
                              (= 1 rw) :G-single
                              (< 0 wr) :G1c
                              (< 0 ww) :G0
                              true (throw (IllegalStateException.
                                            (str "Don't know how to classify"
                                                 (pr-str ex))))))))

    (render-cycle-explanation [_ pair-explainer
                               {:keys [type cycle steps] :as ex}]
      (cycle/render-cycle-explanation
        cycle/cycle-explainer pair-explainer ex))))

(defn cycle-explanations
  "Takes a pair explainer, a function taking an scc and possible yielding a
  cycle, and a series of strongly connected components. Produces a seq (nil if
  empty) of explanations of cycles."
  [pair-explainer cycle-fn sccs]
  (seq (keep (fn [scc]
               (when-let [cycle (cycle-fn scc)]
                 (->> cycle
                      (cycle/explain-cycle cycle-explainer pair-explainer)
                      (cycle/render-cycle-explanation cycle-explainer
                                                      pair-explainer))))
             sccs)))

(defn g0-cases
  "Given a graph, a pair explainer, and a collection of strongly connected
  components, searches for instances of G0 anomalies within it. Returns nil if
  none are present."
  [graph pair-explainer sccs]
  ; For g0, we want to restrict the graph purely to write-write edges.
  (let [g0-graph (-> graph
                     (cycle/remove-relationship :rw)
                     (cycle/remove-relationship :wr))]
    (cycle-explanations pair-explainer
                        (partial cycle/find-cycle g0-graph)
                        sccs)))

(defn g1c-cases
  "Given a graph, an explainer, and a collection of strongly connected
  components, searches for instances of G1c anomalies within them. Returns nil
  if none are present."
  [graph pair-explainer sccs]
  ; For g1c, we want to restrict the graph to write-write edges or write-read
  ; edges. We also need *just* the write-read graph, so that we can
  ; differentiate from G0--this differs from Adya, but we'd like to say
  ; specifically that an anomaly is G1c and NOT G0.
  (let [ww+wr-graph (cycle/remove-relationship graph        :rw)
        wr-graph    (cycle/remove-relationship ww+wr-graph  :ww)]
    (cycle-explanations pair-explainer
                        (partial cycle/find-cycle-starting-with
                                 wr-graph ww+wr-graph)
                        sccs)))

(defn g-single-cases
  "Given a graph, an explainer, and a collection of strongly connected
  components, searches for instances of G-single anomalies within them.
  Returns nil if none are present."
  [graph pair-explainer sccs]
  ; For G-single, we want exactly one rw edge in a cycle, and the remaining
  ; edges from ww or wr.
  (let [rw-graph      (-> graph
                          (cycle/remove-relationship :ww)
                          (cycle/remove-relationship :wr))
        ww+wr-graph   (-> graph
                          (cycle/remove-relationship :rw))]
    (cycle-explanations pair-explainer
                        (partial cycle/find-cycle-starting-with
                                 rw-graph ww+wr-graph)
                        sccs)))

(defn g2-cases
  "Given a graph, an explainer, and a collection of strongly connected
  components, searches for instances of G2 anomalies within them. Returns nil
  if none are present."
  [graph pair-explainer sccs]
  ; For G2, we want at least one rw edge in a cycle; the other edges can be
  ; anything.
  (let [rw-graph (-> graph
                     (cycle/remove-relationship :ww)
                     (cycle/remove-relationship :wr))]
    ; Sort of a hack; we reject cycles that don't have at least two rw edges,
    ; because single rw edges fall under g-single.
    (seq (keep (fn [scc]
                 (when-let [cycle (cycle/find-cycle-starting-with
                                    rw-graph graph scc)]
                   ; Good, we've got a cycle. We're going to reject any cycles
                   ; that are actually G-single, because the G-single checker
                   ; will pick up on those. This could mean we might miss some
                   ; G2 cycles that we COULD find by modifying find-cycle to
                   ; return more candidates, but I don't think it's the end of
                   ; the world; G-single is worse, and if we see it, G2 is
                   ; just icing on the cake
                   (let [cx (cycle/explain-cycle cycle-explainer
                                                 pair-explainer
                                                 cycle)]
                     (when (= :G2 (:type cx))
                       (cycle/render-cycle-explanation cycle-explainer
                                                       pair-explainer cx)))))
               sccs))))

(defn expand-anomalies
  "Takes a collection of anomalies, and returns the fully expanded version of
  those anomalies as a set: e.g. [:G1] -> #{:G0 :G1a :G1b :G1c}"
  [as]
  (let [as (set as)
        as (if (:G2 as)  (conj as :G-single :G1c) as)
        as (if (:G1 as)  (conj as :G1a :G1b :G1c) as)
        as (if (:G1c as) (conj as :G0) as)]
    as))

(defn cycles
  "Performs dependency graph analysis and returns a sequence of anomalies.

  Options:

    :additional-graphs      A collection of graph analyzers (e.g. realtime)
                            which should be merged with our own dependencies.
    :anomalies              A collection of anomalies which should be reported,
                            if found.

  Supported anomalies are :G0, :G1c, :G-single, and :G2. G2 implies G-single
  and G1c, and G1c implies G0."
  ([opts test history checker-opts]
   (let [as       (expand-anomalies (:anomalies opts))
         ; What graph do we need to detect these anomalies?
         analyzer (cond (:G2  as)       graph     ; Need full deps
                        (:G-single as) graph     ; Need full deps
                        (:G1c as)       g1c-graph ; g1c graph includes g0
                        (:G0  as)       ww-graph  ; g0 is just write conflicts
                        true       (throw (IllegalArgumentException.
                                            "Expected at least one anomaly to check for!")))
         ; Any other graphs to merge in?
         analyzer (if-let [ags (:additional-graphs opts)]
                    (apply cycle/combine analyzer ags)
                    analyzer)

         ; Good, analyze the history
         {:keys [graph explainer sccs]} (cycle/check analyzer history)

         ; Find specific anomaly cases
         g0         (when (:G0 as)        (g0-cases graph explainer sccs))
         g1c        (when (:G1c as)       (g1c-cases graph explainer sccs))
         g-single  (when (:G-single as) (g-single-cases
                                            graph explainer sccs))
         g2         (when (:G2 as)        (g2-cases graph explainer sccs))

         ; Results
         anomalies (cond-> {}
                     g0                    (assoc :G0         g0)
                     g1c                   (assoc :G1c        g1c)
                     g-single              (assoc :G-single  g-single)
                     g2                    (assoc :G2         g2))]
     ; Write out cycles as a side effect
     (doseq [[type cycles] anomalies]
       (cycle/write-cycles! test
                            (assoc checker-opts
                                   :filename (str (name type) ".txt"))
                            cycles))
     anomalies)))

(defn checker
  "Full checker for append and read histories. Options are:

    :additional-graphs      A collection of graph analyzers (e.g. realtime)
                            which should be merged with our own dependencies.
    :anomalies              A collection of anomalies which should be reported,
                            if found.

  Supported anomalies are:

    :G0   Write Cycle. A cycle comprised purely of write-write deps.
    :G1a  Aborted Read. A transaction observes data from a failed txn.
    :G1b  Intermediate Read. A transaction observes a value from the middle of
          another transaction.
    :G1c  Circular Information Flow. A cycle comprised of write-write and
          write-read edges.
    :G1   G1a, G1b, and G1c.
    :G2   A dependency cycle with at least one anti-dependency edge.

  :G2 implies :G1c, and :G1c implies :G0, because if we construct the graph for
  G2, it'll include all the edges for G1c, and so on. See
  http://pmg.csail.mit.edu/papers/icde00.pdf for context.

  Note that while we can *find* instances of G0, G1c and G2, we can't (yet)
  categorize them automatically. These anomalies are grouped under :G0+G1c+G2
  in checker results."
  ([]
   (checker {:anomalies [:G1 :G2]}))
  ([opts]
   (let [opts       (update opts :anomalies expand-anomalies)
         anomalies  (:anomalies opts)]
     (reify checker/Checker
       (check [this test history checker-opts]
         (let [history       (remove (comp #{:nemesis} :process) history)
               g1a           (when (:G1a anomalies) (g1a-cases history))
               g1b           (when (:G1b anomalies) (g1b-cases history))
               dups          (duplicates history)
               sorted-values (sorted-values history)
               incmp-order   (incompatible-orders sorted-values)
               cycles        (cycles opts test history checker-opts)
               ; Categorize anomalies and build up a map of types to examples
               anomalies (cond-> cycles
                           dups         (assoc :duplicate-elements dups)
                           incmp-order  (assoc :incompatible-order incmp-order)
                           (seq g1a)    (assoc :G1a g1a)
                           (seq g1b)    (assoc :G1b g1b))]
           (if (empty? anomalies)
             {:valid?         true}
             {:valid?         false
              :anomaly-types  (sort (keys anomalies))
              :anomalies      anomalies})))))))
