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
            [fipp.edn :refer [pprint]]
            [jepsen [checker :as checker]
                    [txn :as txn :refer [reduce-mops]]
                    [util :as util :refer [spy]]]
						[jepsen.tests.cycle :as cycle]
						[jepsen.tests.cycle.txn :as ct]
						[jepsen.txn.micro-op :as mop]
						[knossos.op :as op])
  (:import (io.lacuna.bifurcan IEdge)))

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

(defn initial-state-version-graphs
  "We assume the initial state of every key is `nil`; every version trivially
  comes after it. We take a history and return a map of keys to version graphs
  encoding this relationship."
  [history]
  (reduce (fn op [vgs op]
            (if (or (op/invoke? op)
                    (op/fail?   op))
              vgs ; No sense in inferring anything here
              (let [txn (:value op)
                    writes (txn/ext-writes txn)
                    ; For reads, we only know their values when the op is OK.
                    reads  (when (op/ok? op) (txn/ext-reads txn))]
                (->> (concat writes reads)
                     ; OK, now iterate over kv maps, building up our version
                     ; graph.
                     (reduce (fn kv [vgs [k v]]
                               (if (nil? v)
                                 ; Don't make a self-edge!
                                 vgs
                                 (let [vg (get vgs k (cycle/digraph))]
                                   (assoc vgs k (.link vg nil v)))))
                               vgs)))))
          {}
          history))

(defn wfr-version-graphs
  "If we assume that within a transaction, writes follow reads, then we can
  infer the version order wherever a transaction performs an external read of
  some key x, and an external write of x as well."
  [history]
  (->> history
       (filter op/ok?) ; Since we need BOTH reads and writes, this only works
                       ; with ok ops.
       (reduce
         (fn [vgs op]
           (let [txn    (:value op)
                 reads  (txn/ext-reads txn)
                 writes (txn/ext-writes txn)]
             (reduce (fn [vgs [k v]]
                       (if (not (nil? v)) ; nils handled separately
                         (if-let [v' (get writes k)]
                           (let [vg (get vgs k (cycle/digraph))]
                             (assoc vgs k (.link vg v v')))
                           vgs)
                         vgs))
                     vgs
                     reads)))
         {})))

(defn transaction-graph->version-graphs
  "Takes a graph of transactions (operations), and yields a map of keys to
  graphs of version relationships we can infer from those transactions,
  ASSUMING THAT IF T1 < T2, x1 < x2, FOR ALL x1 in T1 and x2 in T2.

  For instance, if a system is per-key sequential, you could feed this a
  process order and use it to derive a version order, since each process must
  observe subsequent states of the system. Likewise, if a system is per-key
  linearizable, you can use a realtime order to infer version relationships.

  Assume we have a transaction T1 which interacts with key k at value v1. We
  write every other transation as T2, T3, etc if it interacts with k, and t2,
  t3, etc if it does not. We write the first version of k interacted with by T2
  as v2, T3 as v3, and so on.

    T1─T2

  If T2 interacts with k at v2, we can infer v1 < v2. Great.

    T1─t2─T3

  Since t2 does not interact with k, this doesn't help us--but T3 *does*, so we
  can infer v1 < v3.

       ┌T2
    T1─┤
       └T3

  When a dependency chain splits, we have (at most) n dependencies we can
  infer: x1 < x2, x1 < x3.

       ┌T2
    T1─┤
       └t3─T4

  Clearly we can infer x1 < x2--and we need search no further, since T2's
  dependencies will cover the rest of the chain transitively. We must search
  past t3 however, to T4, to infer x1 < x4.

  In general, then, our program is to search the downstream transaction graph,
  stopping and recording a dependency whenever we encounter a transaction which
  interacted with k.

  This search is, of course, n^2: the first transaction might have to search
  every remaining transaction, the second the same chain, and so forth. This is
  *especially* likely to explode pathologically because we probably abandon
  keys at some point in the history, leaving a long stretch of transactions to
  fruitlessly explore.

  To avoid this problem, we *compress* the transaction graph to remove
  transactions which do not involve k, in linear time. We simply find a
  transaction t2 which does not involve k, take all of t2's predecessors, all
  of t2's successors, and link them all directly together, finally deleting t2.
  This process is linear in the number of keys, transactions, and edges.

  With this process complete, we can transform the reduced transaction graph
  directly to a version graph: the final value x1 in T1 directly precedes the
  initial values x2, x3, ... in T2, T3, ... following T1."
  [txn-graph]
  (->> txn-graph
       ct/all-keys
       (pmap (fn per-key [k]
               ; Restrict the general graph to just those which externally
               ; interact with k.
               (let [touches-k? (fn touches-k? [op]
                                  ; We can't infer anything from
                                  ; invocations/fails.
                                  (when (or (op/ok? op) (op/info? op))
                                    (let [txn    (:value op)
                                          ; Can't infer crashed reads!
                                          reads  (when (op/ok? op)
                                                   (txn/ext-reads txn))
                                          ; We can infer crashed writes though!
                                          writes (txn/ext-writes txn)]
                                    (or (contains? reads k)
                                        (contains? writes k)))))
                     ; Our graph-collapsing algorithm is gonna HAMMER our
                     ; predicate for whether a txn touches k, so we precompute
                     ; the fuck out of it
                     touches-k? (->> (.vertices txn-graph)
                                     (filter touches-k?)
                                     set)
                     key-graph (->> txn-graph
                                    (cycle/collapse-graph touches-k?))]
                 ; Now, take every op in the key-restricted graph...
                 (->> key-graph
                      (reduce
                        (fn map-graph [version-graph op]
                          ; We're trying to relate *external* values forward.
                          ; There are two possible external values per key per
                          ; txn: the first read, and the final write. WFR
                          ; (which we TODO check separately) lets us infer the
                          ; relationship between the first read and final write
                          ; *in* the transaction, so what we want to infer here
                          ; is the relationship between the *final* external
                          ; value. If internal consistency holds (which we
                          ; check separately), then the final external value
                          ; must be the final write, or if that's not present,
                          ; the first read.
                          (let [txn (:value op)
                                v1 (or (get (txn/ext-writes txn) k)
                                       ; Reads only fix a value when ok!
                                       (and (op/ok? op)
                                            (get (txn/ext-reads txn) k)))
                                ; Now, we want to relate this to the first
                                ; external value of k for every subsequent
                                ; transaction in the graph, which will be
                                ; either the external read, or if that's
                                ; missing, the external write.
                                ;
                                ; TODO: as an optimization, we could precompute
                                ; external reads and writes for each txn and
                                ; avoid re-traversing the txn every time.
                                v2s (->> (cycle/out key-graph op)
                                         (map (fn descendent-value [op]
                                                (let [txn (:value op)]
                                                  ; Likewise, reads only fix a
                                                  ; value when ok
                                                  (or (and (op/ok? op)
                                                           (get (txn/ext-reads
                                                                  txn) k))
                                                      (get (txn/ext-writes txn) k)))))
                                         set)
                                ; Don't generate self-edges, of course!
                                v2s (disj v2s v1)]
                            ; Great, link those together.
                            (cycle/link-to-all version-graph v1 v2s)))
                        ; Start reduction with an empty graph
                        (cycle/linear (cycle/digraph)))
                      ; And make a [k graph] pair
                      (vector k)))))
       (into {})
       (util/map-vals cycle/forked)))

(defn cyclic-version-cases
  "Given a map of version graphs, returns a sequence (or nil) of cycles in that
  graph."
  [version-graphs]
  (seq
    (reduce (fn [cases [k version-graph]]
              (let [sccs (cycle/strongly-connected-components version-graph)]
                (->> sccs
                     (sort-by (partial reduce min))
                     (map (fn [scc]
                            {:key k
                             :scc scc}))
                     (into cases))))
            []
            version-graphs)))

(defn version-graphs
  "We build version graphs by combining information from many sources. This
  function takes options (as for ww+rw-graph) and a history, and yields
  {:anomalies [...], :sources [...], :graphs version-graphs}"
  [opts history]
  (loop [analyzers (cond-> [{:name    :initial-state
                             :grapher initial-state-version-graphs}]

                     (:wfr-keys? opts)
                     (conj {:name     :wfr-keys
                            :grapher  wfr-version-graphs})

                     (:sequential-keys? opts)
                     (conj {:name    :sequential-keys
                            :grapher (comp transaction-graph->version-graphs
                                           :graph
                                           cycle/process-graph)})

                     (:linearizable-keys? opts)
                     (conj {:name    :linearizable-keys
                            :grapher (comp transaction-graph->version-graphs
                                           :graph
                                           cycle/realtime-graph)}))
         sources       []
         graphs        {}
         cycles        []]
    (if (seq analyzers)
      (let [{:keys [name grapher]} (first analyzers)
            ; Apply this grapher fn to the history, merge it into the graphs,
            ; and record any cyclic version cases we might find, along with the
            ; analyzer names that contributed to that cycle.
            ;
            ; TODO: This is basically the txn graph explainer cycle search
            ; problem all over again, just over versions. I'm writing a hack
            ; here because the paper deadline is coming up RIGHT NOW, but later
            ; we should come back and redo this so we can *justify* exactly why
            ; we inferred a version order.
            sources'  (conj sources name)
            graph     (grapher history)
            graphs'   (merge-with cycle/digraph-union graphs graph)]
        (if-let [cs (->> (cyclic-version-cases graphs')
                         (map #(assoc % :sources sources'))
                         seq)]
          ; Huh. Cycles in this version of the dependency graph. Let's skip
          ; over it in the graph, but note the cycle anomalies.
          (recur (next analyzers) sources graphs (into cycles cs))
          ; No cycles in this combined graph; let's use it!
          (recur (next analyzers) sources' graphs' cycles)))
      ; Done!
      {:anomalies (when (seq cycles) {:cyclic-versions cycles})
       :sources   sources
       :graphs    graphs})))

(defn version-graphs->transaction-graph
  "Takes a history and a map of keys to version graphs, and infers a graph of
  dependencies between operations in that history by using the (likely partial)
  version graph information.

  We do this by taking every key k, and for k, every edge v1 -> v2 in the
  version graph, and for every T1 which finally interacted with v1, and every
  T2 which initially interacted with T2, emitting an edge in the transaction
  graph. We tag our edges :ww and :rw as appropriate. :wr edges we can detect
  directly; we don't need the version graph to do that. Any :rr edge SHOULD
  (assuming values just don't pop out of nowhere, internal consistency holds,
  etc) manifest as a combination of :rw, :ww, and :wr edges; we don't gain
  anything by emitting them here."
  [history version-graphs]
  (let [ext-read-index  (ext-index txn/ext-reads  history)
        ext-write-index (ext-index txn/ext-writes history)]
    (reduce
      (fn per-key [g [k version-graph]]
        (reduce (fn [g ^IEdge edge]
                  (let [v1        (.from edge)
                        v2        (.to edge)
                        k-writes  (get ext-write-index k)
                        k-reads   (get ext-read-index k)
                        v1-reads  (get k-reads v1)
                        v1-writes (get k-writes v1)
                        v2-writes (get k-writes v2)
                        all-vals  (set (concat v1-reads v1-writes v2-writes))]
                    (-> g
                        (cycle/link-all-to-all v1-writes v2-writes :ww)
                        (cycle/link-all-to-all v1-reads  v2-writes :rw)
                        (cycle/remove-self-edges all-vals))))
                g
                (cycle/edges version-graph)))
      (cycle/digraph)
      version-graphs)))

(defn explain-op-deps
  "Given version graphs, a function extracting a map of keys to values from op
  A, and also from op B, and a pair of operations A and B, returns a map (or
  nil) explaining why A precedes B.

  We look for a key on which A interacted with version v, and B interacted with
  v', and v->v' in the version graph."
  [version-graphs ext-a a ext-b b]
  (let [a-kvs (ext-a (:value a))
        b-kvs (ext-b (:value b))]
    ; Look for a key where a's value precedes b's!
    (first
      (keep (fn [[k a-value]]
              (when-let [version-graph (get version-graphs k)]
                (when-let [b-value (get b-kvs k)]
                  (when (.contains (cycle/out version-graph a-value)
                                   b-value)
                    {:key     k
                     :value   a-value
                     :value'  b-value}))))
            a-kvs))))

(defrecord WWExplainer [version-graphs]
  cycle/DataExplainer
  (explain-pair-data [_ a b]
    (when-let [e (explain-op-deps version-graphs
                                  txn/ext-writes a txn/ext-writes b)]
      (assoc e :type :ww)))

  (render-explanation [_ {:keys [key value value'] :as m} a-name b-name]
    (str a-name " set key " (pr-str key) " to " (pr-str value) ", and "
         b-name " set it to " (pr-str value')
         ", which came later in the version order")))

(defrecord RWExplainer [version-graphs]
  cycle/DataExplainer
  (explain-pair-data [_ a b]
    (when-let [e (explain-op-deps version-graphs
                                  txn/ext-reads a txn/ext-writes b)]
      (assoc e :type :rw)))

  (render-explanation [_ {:keys [key value value'] :as m} a-name b-name]
    (str a-name " read key " (pr-str key) " = " (pr-str value) ", and "
         b-name " set it to " (pr-str value')
         ", which came later in the version order")))

(defn ww+rw-graph
  "Given options and a history where the ops are txns, constructs a graph and
  explainer over transactions with :rw and :ww edges, based on an inferred
  partial version order.

  We infer the version order based on options:

    :linearizable-keys?  Uses realtime order
    :sequential-keys?    Uses process order
    :wfr-keys?           Assumes writes follow reads in a txn

  In addition, we infer a dependency edge from nil to every non-nil value."
  [opts history]
  (let [{:keys [anomalies sources graphs]} (version-graphs opts history)
        tg  (version-graphs->transaction-graph history graphs)]
    ; We might have found anomalies when computing the version graph
    {:anomalies anomalies
     :graph     tg
     :explainer (cycle/->CombinedExplainer [(WWExplainer. graphs)
                                            (RWExplainer. graphs)])}))

(defrecord WRExplainer []
  cycle/DataExplainer
  (explain-pair-data [_ a b]
    (let [writes (txn/ext-writes (:value a))
          reads  (txn/ext-reads  (:value b))]
      (reduce (fn [_ [k v]]
                (when (and (contains? reads k)
                           (= v (get reads k)))
                  (reduced
                    {:type  :wr
                     :key   k
                     :value v})))
              nil
              writes)))

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
    {:graph
     (.forked
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
               (cycle/linear (cycle/digraph))
               ext-reads))
     :explainer (WRExplainer.)}))

(defn graph
  "Given options and a history, computes a {:graph g, :explainer e} map of
  dependencies. We combine several pieces:

    1. A wr-dependency graph, which we obtain by directly comparing writes and
       reads.

    2. Additional graphs, as given by (:additional-graphs opts).

    3. ww and rw dependencies, as derived from a version order, which we derive
       on the basis of...

       a. nil precedes *every* read value

       b. If either :linearizable-keys? or :sequential-keys? is passed, we
          assume individual keys are linearizable/sequentially consistent, and
          use that to infer (partial) version graphs from either the realtime
          or process order, respectively.

  The graph we return combines all this information.

  TODO: maybe use writes-follow-reads(?) to infer more versions from wr deps?"
  [opts history]
  (let [; Build our combined analyzers
        analyzers (into [wr-graph (partial ww+rw-graph opts)]
                        (:additional-graphs opts))
        analyzer (apply cycle/combine analyzers)]
    ; And go!
    (analyzer history)))

(defn checker
  "Full checker for write-read registers. Options are:

    :additional-graphs      A collection of graph analyzers (e.g. realtime)
                            which should be merged with our own dependency
                            graph.
    :anomalies              A collection of anomalies which should be reported,
                            if found.
    :sequential-keys?       Assume that each key is independently sequentially
                            consistent, and use each processes' transaction
                            order to derive a version order.
    :linearizable-keys?     Assume that each key is independently linearizable,
                            and use the realtime process order to derive a
                            version order.
    :wfr-keys?              Assume that within each transaction, writes follow
                            reads, and use that to infer a version order.

  Supported anomalies are:

    :G0   Write Cycle. A cycle comprised purely of write-write deps.
    :G1a  Aborted Read. A transaction observes data from a failed txn.
    :G1b  Intermediate Read. A transaction observes a value from the middle of
          another transaction.
    :G1c  Circular information flow. A cycle comprised of write-write and
          write-read edges.
    :G-single  An dependency cycle with exactly one anti-dependency edge.
    :G2   A dependency cycle with at least one anti-dependency edge.
    :internal Internal consistency anomalies. A transaction fails to observe
              state consistent with its own prior reads or writes.

  :G2 implies :G-single and :G1c. :G1 implies :G1a, :G1b, and :G1c. G1c implies
  G0. The default is [:G2 :G1a :G1b :internal], which catches everything."
  ([]
   (checker {}))
  ([opts]
   (let [anomalies  (ct/expand-anomalies
                      (get opts :anomalies [:G2 :G1a :G1b :internal]))
         opts       (assoc opts :anomalies anomalies)]
     (reify checker/Checker
       (check [this test history checker-opts]
         (let [history  (remove (comp #{:nemesis} :process) history)
               _        (ct/assert-type-sanity history)
               g1a      (when (:G1a anomalies) (g1a-cases history))
               g1b      (when (:G1b anomalies) (g1b-cases history))
               internal (when (:internal anomalies) (internal-cases history))
               cycles   (ct/cycles! opts test (partial graph opts) history
                                        checker-opts)
               ; Build up anomaly map
               anomalies (cond-> cycles
                           internal (assoc :internal internal)
                           g1a      (assoc :G1a g1a)
                           g1b      (assoc :G1b g1b))]
           ; And results!
           (if (empty? anomalies)
             {:valid? true}
             {:valid?         (if (every? ct/unknown-anomaly-types
                                          (keys anomalies))
                                :unknown
                                false)
              :anomaly-types  (sort (keys anomalies))
              :anomalies      anomalies})))))))
