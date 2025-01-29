(ns jepsen.dgraph.sequential
  "Verifies sequentialish consistency by ensuring that each process observes
  monotonic states.

  Dgraph provides snapshot isolation, but snapshot isolation allows reads to
  observe arbitrarily stale values, so long as writes do not conflict. In
  particular, there is no guarantee that reads will observe logically monotonic
  states of the system. From https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf:

  > ... each transaction reads data from a snapshot of the (committed) data as
  > of the time the transaction started, called its Start-Timestamp. This time
  > may be any time before the transaction's first Read.

  We wish to verify a different sort of property, which I'm going to call
  *sequential consistency*, but isn't exactly.

  Sequential implies that there exists a total order of transactions which is
  consistent with the order on each process. Snapshot isolation, however, does
  not necessarily admit any totally ordered history; that would require
  serializability.

  What we're going to do here is restrict ourselves to transactions of two
  forms:

  1. Read-only transactions
  2. Transactions where every read key is also written

  I would like to argue that this is sufficient to imply serializability, by
  making hand-waving gestures and muttering about materializing conflicts, and
  observing that the example of a read-only nonserializable history in
  https://www.cs.umb.edu/~poneil/ROAnom.pdf requires a transaction which
  doesn't write its full read set.

  So, if we allow that a total transaction order *does* exist, then we can
  construct a transactional analogy for sequential consistency: there exists a
  total order of transactions which is compatible with the order of
  transactions on every given process.

  To check this, we perform two types of transactions on a register: a read
  transaction, and a read, increment, and write transaction. These are of type
  1 and type 2 respectively, so our histories ought to be serializable. Since
  no transaction can *lower* the value of the register, once a value is
  observed by a process, that process should observe that value or higher from
  that point forward.

  To verify this, we'll record the resulting value of the register in the
  :value for each operation, and ensure that in each process, those values are
  monotonic."
  (:require [clojure.tools.logging :refer [info]]
            [clojure.core.reducers :as r]
            [dom-top.core :refer [with-retry]]
            [knossos.op :as op]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [independent :as independent]
                    [util :as util]
                    [store :as store]]
            [jepsen.generator.pure :as gen]
            [jepsen.checker.timeline :as timeline]
            [jepsen.checker.perf :as perf]
            [gnuplot.core :as g]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "key:   int @index(int)"
                               (when (:upsert-schema test) " @upsert")
                               " .\n"
                               "value: int @index(int) .\n")))

  (invoke! [this test op]
    (let [[k _] (:value op)]
      (c/with-conflict-as-fail op
        (c/with-txn [t conn]
          (case (:f op)
            :inc (let [{:keys [uid value] :or {value 0}}
                       (->> (c/query t "{ q(func: eq(key, $key)) {
                                       uid, value
                                       }}"
                                     {:key k})
                            :q
                            first)
                       value (inc value)]
                   (if uid
                     (c/mutate! t {:uid uid, :value value})
                     (c/mutate! t {:key k,   :value value}))
                   (assoc op :type :ok, :value (independent/tuple k value)))

            :read (-> (c/query t "{ q(func: eq(key, $key)) { uid, value } }"
                               {:key k})
                      :q
                      first
                      :value
                      (or 0)
                      (->> (independent/tuple k)
                           (assoc op :type :ok, :value))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn non-monotonic-pairs
  "Given a history, finds pairs of ops on the same process where the value
  decreases."
  [history]
  (->> history
       (filter op/ok?)
       (reduce (fn [[last errs] op]
                 ; Last is a map of process ids to the last
                 ; operation we saw for that process. Errs is a
                 ; collection of error maps.
                 (let [p          (:process op)
                       value      (:value op)
                       last-value (-> (last p) :value (or 0))]
                   (if (<= last-value value)
                     ; Monotonic
                     [(assoc last p op) errs]
                     ; Non-monotonic!
                     [(assoc last p op)
                      (conj errs [(last p) op])])))
               [{} []])
       second))

(defn checker
  "This checks a single register; we generalize it using independent/checker."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [errs (non-monotonic-pairs history)]
        {:valid? (empty? errs)
         :non-monotonic errs}))))

(defn merged-windows
  "Takes a collection of points, and computes [lower, upper] windows of s
  elements before and after each point, then merges overlapping windows
  together."
  [s points]
  (when (seq points)
    (let [points (sort points)
          ; Build up a vector of windows by keeping track of the current lower
          ; and upper bounds, expanding upper whenever necessary.
          [windows lower upper]
          (reduce (fn [[windows lower upper] p]
                    (let [lower' (- p s)
                          upper' (+ p s)]
                      (if (<= upper lower')
                        ; Start a new window
                        [(conj windows [lower upper]) lower' upper']
                        ; Expand this window
                        [windows lower upper'])))
                  [[] (- (first points) s) (+ (first points) s)]
                  points)]
      (conj windows [lower upper]))))

(defn plot!
  "Renders a plot of a history to the given file. Takes a test and checker opts
  to determine the subdirectory to write to."
  [test opts filename history]
  (let [series (->> history
                    (r/filter (comp number? :process))
                    (r/filter #(= :ok (:type %)))
                    (group-by :process)
                    (util/map-vals
                      (partial mapv (fn [op]
                                      [(util/nanos->secs (:time op))
                                       (:value op)]))))
        colors (perf/qs->colors (keys series))
        path (.getCanonicalPath
               (store/path! test (:subdirectory opts)
                            (str "sequential " filename ".png")))]
    (try
      (g/raw-plot!
        (concat (perf/preamble path)
                (perf/nemesis-regions history)
                (perf/nemesis-lines history)
                [['set 'title (str (:name test) " sequential by process")]
                 '[set ylabel "register value"]
                 ['plot (apply g/list
                               (for [[process points] series]
                                 ["-"
                                  'with       'linespoints
                                  'pointtype  2
                                  'linetype   (colors process)
                                  'title      (str process)]))]])
        (vals series))
      {:valid? true}
      (catch java.io.IOException _
        (throw (IllegalStateException. "Error rendering plot; verify gnuplot is installed and reachable"))))))

(defn plotter
  "Plots interesting bits of the value as seen by each process history."
  []
  (reify checker/Checker
    (check [this test history opts]
      ; Identify interesting regions
      (let [history (->> history
                         (r/filter (fn [op]
                                     (or (op/ok? op)
                                         (= :nemesis (:process op)))))
                         (into []))
            spots (nth (reduce (fn [[i last spots] op]
                                 (if (= :nemesis (:process op))
                                   ; Skip nemesis
                                   [(inc i) last spots]
                                   ; Figure out if this is a spot
                                   (let [p  (:process op)
                                         v  (-> (last p) :value (or 0))
                                         v' (:value op)]
                                     [(inc i)
                                      (assoc last p op)
                                      (if (<= v v')
                                        ; Monotonic
                                        spots
                                        (conj spots i))])))
                               [0 {} []]
                               history)
                       2)]
        (->> spots
             (merged-windows 32)
             (map-indexed (fn [i [lower upper]]
                            (->> (subvec history
                                         (max lower 0)
                                         (min upper (dec (count history))))
                                 (plot! test opts i))))
             dorun))
      {:valid? true})))

(defn inc-gen  [_ _]
  {:type :invoke, :f :inc, :value (independent/tuple (rand-int 8) nil)})
(defn read-gen [_ _]
  {:type :invoke, :f :read, :value (independent/tuple (rand-int 8) nil)})

(defn workload
  "Stuff you need to build a test!"
  [opts]
  {:client    (Client. nil)
   :checker   (independent/checker
                (checker/compose
                  {:sequential (checker)
                   :plot       (plotter)
                   :timeline   (timeline/html)}))
   :generator (gen/mix [inc-gen read-gen])})
