(ns jepsen.tests.cycle.wr
  "A test which looks for cycles in write/read transactions. Writes are assumed
  to be unique, but this is the only constraint.

  Unlike the append test, we cannot recover information about the version order
  by comparing versions directly. The only dependency we can directly infer
  between transactions is:

    1. T1 writes x_i, and T2 reads x_i: we know T1 < T2.

  However, this is not the only information we have access to. We can infer
  partial version orders *within* a transaction. When x_i =/= x_j...

    2rr. T1 reads x_i,  then reads  x_j: we know x_i < x_j.
    2rw. T1 reads x_i,  then writes x_j: we know x_i < x_j.
    2wr. T1 writes x_i, then reads  x_j: we know x_i < x_j.
    2ww. T1 writes x_i, then writes x_j: we know x_i < x_j.

  In serializable systems, only 2ww and 2rw should arise. 2rr is obviously a
  non-repeatable read. 2wr suggests dirty write, I think. Both 2rr and 2wr are
  violations of internal consistency.

  We can use these dependencies to infer additional transaction edges, wherever
  the version graph is available.

  What's more: given an ordering relationship between two transactions, we can,
  by assuming serializability, infer *additional version constraints*. If T1 <
  T2, and T1 reads or writes x_i, and T2 reads or writes x_j, we can infer x_i
  < x_j. This expands our version graph.

  We can alternate between expanding the transaction graph and expanding the
  version graph until we reach a fixed point.

  Now I just gotta implement this."
  (:require [clojure.core.reducers :as r]
            [jepsen [checker :as checker]
                    [txn :as txn]]
						[jepsen.tests.cycle :as cycle]
						[jepsen.tests.cycle.txn :as ct]
						[jepsen.txn.micro-op :as mop]
						[knossos.op :as op]))

(defn op-internal-case
  "Given an op, returns a map describing internal consistency violations, or
  nil otherwise. Our maps are:

      {:op        The operation which went wrong
       :mop       The micro-operation which went wrong
       :expected  The state we expected to observe.}"
  [op]
  ; We maintain a map of keys to expected states.
  (->> (:value op)
       (reduce (fn [[state error] [f k v :as mop]]
                 (case f
                   :w [(assoc! state k v) error]
                   :r (let [s (get state k)]
                        (if (and s (not= s v))
                          ; Not equal!
                          (reduced [state
                                    {:op       op
                                     :mop      mop
                                     :expected s}])
                          ; OK! Either a match, or our first time seeing k.
                          [(assoc! state k v) error]))))
               [(transient {}) nil])
       second))

(defn internal-cases
  "Given a history, finds operations which exhibit internal consistency
  violations: e.g. some read [:r k v] in the transaction fails to observe a v
  consistent with that transaction's previous write to k."
  [history]
  (ct/ok-keep op-internal-case history))

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
  (let [failed (ct/failed-writes #{:w} history)]
    ; Look for ok ops with a read mop of a failed append
    (->> history
         (filter op/ok?)
         ct/op-mops
         (keep (fn [[op [f k v :as mop]]]
                 (when (= :r f)
                   (when-let [writer (get-in failed [k v])]
                     {:op        op
                      :mop       mop
                      :writer    writer}))))
         seq)))

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
  (let [im (ct/intermediate-writes #{:w} history)]
    ; Look for ok ops with a read mop of an intermediate append
    (->> history
         (filter op/ok?)
         ct/op-mops
         (keep (fn [[op [f k v :as mop]]]
                 (when (= :r f)
									 ; We've got an illegal read if value came from an
				           ; intermediate append.
                   (when-let [writer (get-in im [k v])]
                     ; Internal reads are OK!
                     (when (not= op writer)
                       {:op       op
                        :mop      mop
                        :writer   writer})))))
         seq)))

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
  cycle/DataExplainer
  (explain-pair-data [_ a b]
    (let [writes (txn/ext-writes (:value a))
          reads  (txn/ext-reads  (:value b))]
      (reduce (fn [_ k]
                (let [r (get reads k)
                      w (get writes k)]
                  (when (= r w)
                    (reduced
                      {:type  :wr
                       :key   k
                       :value w}))))
              nil
              (keys reads))))

  (render-explanation [_ {:keys [key value]} a-name b-name]
    (str a-name " wrote " (pr-str key) " = " (pr-str value)
         ", which was read by " b-name)))

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
                               1 (cycle/link-to-all graph (first writes) reads
                                                    :wr)

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
               (cycle/linear (cycle/directed-graph))
               ext-reads))
     (WRExplainer.)]))

(defn cycles!
  "Performs dependency graph analysis and returns a map of anomalies. Writes
  out anomaly files as a side effect. Options are basically those as for
  checker; see checker for context."
  [opts test history checker-opts]
  (let [as (:anomalies opts)

        ; What graph do we need to detect these anomalies?
        analyzer wr-graph

        ; Merge in extra graph analyzers
        analyzer (if-let [ags (:additional-graphs opts)]
                   (apply cycle/combine analyzer ags)
                   analyzer)

        ; Analyze the history
        {:keys [graph explainer sccs]} (cycle/check analyzer history)

        ; TODO: we should probably abort here if the graph is empty, tell the
        ; user we're failing to infer anything interesting.

        ; Find anomalies
        g1c (when (:G1c as) (ct/g1c-cases graph explainer sccs))

        ; Build result map
        anomalies (cond-> {}
                    g1c (assoc :G1c g1c))]

    ; Write out cycles as a side effect.
    (doseq [[type cycles] anomalies]
      (cycle/write-cycles! test
                           (assoc checker-opts
                                  :filename (str (name type) ".txt"))
                           cycles))
    anomalies))

(defn checker
  "Full checker for write-read registers. Options are:

    :additional-graphs      A collection of graph analyzers (e.g. realtime)
                            which should be merged with our own dependency
                            graph.
    :anomalies              A collection of anomalies which should be reported,
                            if found.

  Supported anomalies are:

    :G1a  Aborted Read. A transaction observes data from a failed txn.
    :G1b  Intermediate Read. A transaction observes a value from the middle of
          another transaction.
    :G1c  Circular information flow. A cycle comprised of write-write and
          write-read edges.
    :internal Internal consistency anomalies. A transaction fails to observe
              state consistent with its own prior reads or writes.

  :G1 implies :G1a, :G1b, and :G1c."
  ([]
   (checker {:anomalies [:G1 :internal]}))
  ([opts]
   (let [opts       (update opts :anomalies ct/expand-anomalies)
         anomalies  (:anomalies opts)]
     (reify checker/Checker
       (check [this test history checker-opts]
         (let [history  (remove (comp #{:nemesis} :process) history)
               g1a      (when (:G1a anomalies) (g1a-cases history))
               g1b      (when (:G1b anomalies) (g1b-cases history))
               internal (when (:internal anomalies) (internal-cases history))
               cycles   (cycles! opts test history checker-opts)
               ; Build up anomaly map
               anomalies (cond-> cycles
                           internal (assoc :internal internal)
                           g1a      (assoc :G1a g1a)
                           g1b      (assoc :G1b g1b))]
           (if (empty? anomalies)
             {:valid? true}
             {:valid? false
              :anomaly-types (sort (keys anomalies))
              :anomalies anomalies})))))))
