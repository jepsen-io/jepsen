(ns jepsen.tests.cycle.txn
  "Functions for cycle analysis over transactional workloads."
  (:require [jepsen.txn :as txn :refer [reduce-mops]]
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

(defn expand-anomalies
  "Takes a collection of anomalies, and returns the fully expanded version of
  those anomalies as a set: e.g. [:G1] -> #{:G0 :G1a :G1b :G1c}"
  [as]
  (let [as (set as)
        as (if (:G2 as)  (conj as :G-single :G1c) as)
        as (if (:G1 as)  (conj as :G1a :G1b :G1c) as)
        as (if (:G1c as) (conj as :G0) as)]
    as))
