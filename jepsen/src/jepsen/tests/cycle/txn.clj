(ns jepsen.tests.cycle.txn
  "Functions for cycle analysis over transactional workloads."
  (:require [fipp.edn :refer [pprint]]
            [jepsen.txn :as txn :refer [reduce-mops]]
            [jepsen.tests.cycle :as cycle]
						[knossos.op :as op]))

(defn op-mops
  "A lazy sequence of all [op mop] pairs from a history."
  [history]
  (mapcat (fn [op] (map (fn [mop] [op mop]) (:value op))) history))

(defn ok-keep
	"Given a function of operations, returns a sequence of that function applied
  to all ok operations. Returns nil iff every invocation of f is nil."
  [f history]
  (->> history
       (filter op/ok?)
       (keep f)
       seq))

(defn all-keys
  "A sequence of all keys in the given history."
  [history]
  (->> history op-mops (map (comp second second)) distinct))

(defn failed-writes
  "Returns a map of keys to maps of failed write values to the operations which
  wrote them. Used for detecting aborted reads."
  [write? history]
  (reduce-mops (fn index [failed op [f k v :as mop]]
                 (if (and (op/fail? op)
                          (write? f))
                   (assoc-in failed [k v] op)
                   failed))
               {}
               history))

(defn intermediate-writes
  "Returns a map of keys to maps of intermediate write values to the operations
  which wrote them. Used for detecting intermediate reads."
  [write? history]
  (reduce (fn [im op]
            ; Find intermediate writes for this particular txn by
            ; producing two maps: intermediate keys to values, and
            ; final keys to values in this txn. We shift elements
            ; from final to intermediate when they're overwritten.
            (first
              (reduce (fn [[im final :as state] [f k v]]
                        (if (write? f)
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
          history))

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

(def cycle-types
  "All types of cycles we can detect."
  #{:G0 :G1c :G-single :G2})

(def unknown-anomaly-types
  "Anomalies which cause the analysis to yield :valid? :unknown, rather than
  false."
  #{:empty-transaction-graph})

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
  "Takes an options map, including a set of :anomalies, an analyzer function
  (returning Anomalies), and a history. Analyzes the history and yields
  Anomalies, like :G1c [...]}."
  [opts analyzer history]
  (let [; Analyze the history
        {:keys [graph explainer sccs anomalies]} (cycle/check analyzer history)

        ; Find anomalies
        as  (:anomalies opts)
        g0  (when (:G0 as)        (g0-cases       graph explainer sccs))
        g1c (when (:G1c as)       (g1c-cases      graph explainer sccs))
        g-s (when (:G-single as)  (g-single-cases graph explainer sccs))
        g2  (when (:G2 as)        (g2-cases       graph explainer sccs))]
    ; Merge our cases into the existing anomalies map.
    (cond-> anomalies
      g0  (assoc :G0 g0)
      g1c (assoc :G1c g1c)
      g-s (assoc :G-single g-s)
      g2  (assoc :G2 g2))))

(defn cycles!
  "Like cycles, but writes out files as a side effect."
  [opts test analyzer history checker-opts]
  (let [anomalies (cycles opts analyzer history)]
    (doseq [[type cycles] anomalies]
      (when (cycle-types type)
        (cycle/write-cycles! test
                             (assoc checker-opts
                                    :filename (str (name type) ".txt"))
                             cycles)))
    anomalies))
