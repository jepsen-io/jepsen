(ns jepsen.checker
  "Validates that a history is correct with respect to some model."
  (:refer-clojure :exclude [set])
  (:require [clojure.stacktrace :as trace]
            [clojure.core :as core]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [potemkin :refer [definterface+]]
            [jepsen.util :as util :refer [meh fraction map-kv]]
            [jepsen.store :as store]
            [jepsen.checker [perf :as perf]
                            [clock :as clock]]
            [multiset.core :as multiset]
            [gnuplot.core :as g]
            [knossos [model :as model]
                     [op :as op]
                     [competition :as competition]
                     [linear :as linear]
                     [wgl :as wgl]
                     [history :as history]]
            [knossos.linear.report :as linear.report])
  (:import (java.util.concurrent Semaphore)))

(def valid-priorities
  "A map of :valid? values to their importance. Larger numbers are considered
  more signficant and dominate when checkers are composed."
  {true      0
   false     1
   :unknown  0.5})

(defn merge-valid
  "Merge n :valid values, yielding the one with the highest priority."
  [valids]
  (reduce (fn [v1 v2]
            (let [p1 (or (valid-priorities v1)
                         (throw (IllegalArgumentException.
                                  (str (pr-str v1)
                                      " is not a known valid? value"))))
                  p2 (or (valid-priorities v2)
                         (throw (IllegalArgumentException.
                                  (str (pr-str v2)
                                       " is not a known valid? value"))))]
              (if (< p1 p2) v2 v1)))
          true
          valids))

(defprotocol Checker
  (check [checker test history opts]
         "Verify the history is correct. Returns a map like

         {:valid? true}

         or

         {:valid?       false
          :some-details ...
          :failed-at    [details of specific operations]}

         Opts is a map of options controlling checker execution. Keys include:

         :subdirectory - A directory within this test's store directory where
                         output files should be written. Defaults to nil.

          DEPRECATED Checkers should now implement the 4-arity check method
          without model. If the checker still needs a model, provide it at
          construction rather than as an argument to `Checker/check`. See the
          queue and linearizable checkers for examples."))

(defn noop
  "An empty checker that only returns nil."
  []
  (reify Checker
    (check [_ _ _ _])))

(defn check-safe
  "Like check, but wraps exceptions up and returns them as a map like

  {:valid? :unknown :error \"...\"}"
  ([checker test history]
   (check-safe checker test history {}))
  ([checker test history opts]
   (try (check checker test history opts)
        (catch Exception t
          (warn t "Error while checking history:")
          {:valid? :unknown
           :error (with-out-str (trace/print-cause-trace t))}))))

(defn compose
  "Takes a map of names to checkers, and returns a checker which runs each
  check (possibly in parallel) and returns a map of names to results; plus a
  top-level :valid? key which is true iff every checker considered the history
  valid."
  [checker-map]
  (reify Checker
    (check [this test history opts]
      (let [results (->> checker-map
                         (pmap (fn [[k checker]]
                                 [k (check-safe checker test history opts)]))
                         (into {}))]
        (assoc results :valid? (merge-valid (map :valid? (vals results))))))))

(defn concurrency-limit
  "Takes positive integer limit and a checker. Puts an upper bound on the
  number of concurrent executions of this checker. Use this when a checker is
  particularly thread or memory intensive, to reduce context switching and
  memory cost."
  [limit checker]
  ; We use a fair semaphore here because we want checkers to finish ASAP so
  ; they can release their memory, and because we don't invoke check that
  ; often.
  (let [sem (Semaphore. limit true)]
    (reify Checker
      (check [this test history opts]
        (try (.acquire sem)
             (check checker test history opts)
             (finally
               (.release sem)))))))

(defn unbridled-optimism
  "Everything is awesoooommmmme!"
  []
  (reify Checker
    (check [this test history opts] {:valid? true})))

(defn linearizable
  "Validates linearizability with Knossos. Defaults to the competition checker,
  but can be controlled by passing either :linear or :wgl.

  Takes an options map for arguments, ex.
  {:model (model/cas-register)
   :algorithm :wgl}"
  [{:keys [algorithm model]}]
  (assert model
          (str "The linearizable checker requires a model. It received: "
               model
               " instead."))
  (reify Checker
    (check [this test history opts]
      (let [a ((case algorithm
                 :linear       linear/analysis
                 :wgl          wgl/analysis
                 competition/analysis)
               model history)]
        (when-not (:valid? a)
          (try
            ;; Renderer can't handle really broad concurrencies yet
            (linear.report/render-analysis!
             history a (.getCanonicalPath
                        (store/path! test (:subdirectory opts)
                                     "linear.svg")))
            (catch Throwable e
              (warn e "Error rendering linearizability analysis"))))
        ;; Writing these can take *hours* so we truncate
        (assoc a
               :final-paths (take 10 (:final-paths a))
               :configs     (take 10 (:configs a)))))))

(defn queue
  "Every dequeue must come from somewhere. Validates queue operations by
  assuming every non-failing enqueue succeeded, and only OK dequeues succeeded,
  then reducing the model with that history. Every subhistory of every queue
  should obey this property. Should probably be used with an unordered queue
  model, because we don't look for alternate orderings. O(n)."
  [model]
  (reify Checker
    (check [this test history opts]
      (let [final (->> history
                       (r/filter (fn select [op]
                                   (condp = (:f op)
                                     :enqueue (op/invoke? op)
                                     :dequeue (op/ok? op)
                                     false)))
                       (reduce model/step model))]
        (if (model/inconsistent? final)
          {:valid? false
           :error  (:msg final)}
          {:valid?      true
           :final-queue final})))))

(defn set
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted."
  []
  (reify Checker
    (check [this test history opts]
      (let [attempts (->> history
                          (r/filter op/invoke?)
                          (r/filter #(= :add (:f %)))
                          (r/map :value)
                          (into #{}))
            adds (->> history
                      (r/filter op/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            final-read (->> history
                            (r/filter op/ok?)
                            (r/filter #(= :read (:f %)))
                            (r/map :value)
                            (reduce (fn [_ x] x) nil))]
        (if-not final-read
          {:valid? :unknown
           :error  "Set was never read"}

          (let [final-read (core/set final-read)

                ; The OK set is every read value which we tried to add
                ok          (set/intersection final-read attempts)

                ; Unexpected records are those we *never* attempted.
                unexpected  (set/difference final-read attempts)

                ; Lost records are those we definitely added but weren't read
                lost        (set/difference adds final-read)

                ; Recovered records are those where we didn't know if the add
                ; succeeded or not, but we found them in the final set.
                recovered   (set/difference ok adds)]

            {:valid?              (and (empty? lost) (empty? unexpected))
             :attempt-count       (count attempts)
             :acknowledged-count  (count adds)
             :ok-count            (count ok)
             :lost-count          (count lost)
             :recovered-count     (count recovered)
             :unexpected-count    (count unexpected)
             :ok                  (util/integer-interval-set-str ok)
             :lost                (util/integer-interval-set-str lost)
             :unexpected          (util/integer-interval-set-str unexpected)
             :recovered           (util/integer-interval-set-str recovered)}))))))


(definterface+ ISetFullElement
  (set-full-add [element-state op])
  (set-full-read-present [element-state inv op])
  (set-full-read-absent  [element-state inv op]))

; Tracks the state of each element for set-full analysis
;
; We're looking for a few key points here:
;
; The add time is inferred from either the add *or* the first read to observe
; the op, whichever finishes first.
;
; To find the *stable time*, we need to know the most recent missing
; invocation. If we have any successful read invocation *after* it, then we know
; the record was stable.
;
; To find the *lost time*, we need to know the most recent observed invocation.
; If we have any lost invocation *more* recent than that, then we know that the
; record was lost.
(defrecord SetFullElement [element
                           known
                           last-present
                           last-absent]
  ISetFullElement
  (set-full-add [this op]
    (condp = (:type op)
      ; Record the completion of the add op
      :ok (assoc this :known (or known op))
      this))

  (set-full-read-present [this iop op]
    (assoc this
           :known (or known op)

           :last-present
           (if (or (nil? last-present)
                   (< (:index last-present) (:index iop)))
             iop
             last-present)))

  (set-full-read-absent [this iop op]
    (if (or (nil? last-absent)
            (< (:index last-absent) (:index iop)))
      (assoc this :last-absent iop)
      this)))

(defn set-full-element
  "Given an add invocation, constructs a new set element state record to track
  that element"
  [op]
  (map->SetFullElement {:element (:value op)}))

(defn set-full-element-results
  "Takes a SetFullElement and computes a map of final results from it:

      :element         The element itself
      :outcome         :stable, :lost, :never-read
      :lost-latency
      :stable-latency"
  [e]
  (let [known        (:known e)
        known-time   (:time (:known e))
        last-present (:last-present e)
        last-absent (:last-absent e)

        stable?     (boolean
                      (and last-present
                           (< (:index last-absent -1)
                              (:index last-present))))
        ; Note that there exist two asymmetries here.
        ; First, an element has to be known in order for its absence to mean
        ; anything. If it is never confirmed nor observed, it's ok for it not
        ; to exist. We check to make sure the element is actually known.

        ; Second, if a read concurrent with the add of an element e observes e,
        ; then we know e exists. However, a concurrent read which *fails* to
        ; observe e could have linearized before the add. We check the
        ; concurrency windows to make sure the last lost operation didn't
        ; overlap with the known complete time; if the most recent failed read
        ; was also *concurrent* with the add, we call that never-read, rather
        ; than lost.
        lost?       (boolean
                      (and known
                           last-absent
                           (< (:index last-present -1)
                              (:index last-absent))
                           (< (:index (:known e))
                              (:index last-absent))))
        never-read  (not (or stable? lost?))

        ; TODO: 0 isn't really right; we'd need to track the first present
        ; invocations to get these times.
        ; TODO: We should also be smarter about
        ; getting the first absent invocation
        ; *after* the most recent present invocation
        stable-time (when stable?
                      (if last-absent (inc (:time last-absent)) 0))
        lost-time   (when lost?
                      (if last-present (inc (:time last-present)) 0))

        stable-latency (when stable?
                         (-> stable-time (- known-time) (max 0)
                             util/nanos->ms long))
        lost-latency   (when lost?
                         (-> lost-time (- known-time) (max 0)
                             util/nanos->ms long))]
    {:element         (:element e)
     :outcome         (cond stable?     :stable
                            lost?       :lost
                      never-read  :never-read)
     :stable-latency  stable-latency
     :lost-latency    lost-latency
     :known           known
     :last-absent     last-absent}))

(defn frequency-distribution
  "Computes a map of percentiles (0--1, not 0--100, we're not monsters) of a
  collection of numbers, taken at percentiles `points`. If the collection is
  empty, returns nil."
  [points c]
  (let [sorted (sort c)]
    (when (seq sorted)
      (let [n (clojure.core/count sorted)
            extract (fn [point]
                      (let [idx (min (dec n) (int (Math/floor (* n point))))]
                        (nth sorted idx)))]
        (->> points (map extract) (zipmap points) (into (sorted-map)))))))

(defn set-full-results
  "Takes options from set-full, and a collection of SetFullElements. Computes
  agggregate results; see set-full for details."
  [opts elements]
  (let [rs                (mapv set-full-element-results elements)
        outcomes          (group-by :outcome rs)
        stale             (->> (:stable outcomes)
                               (filter (comp pos? :stable-latency)))
        worst-stale       (->> stale
                               (sort-by :stable-latency)
                               reverse
                               (take 8))
        stable-latencies  (keep :stable-latency rs)
        lost-latencies    (keep :lost-latency rs)
        m {:valid?             (cond (< 0 (count (:lost outcomes)))   false
                                     (= 0 (count (:stable outcomes))) :unknown
                                     (and (:linearizable? opts)
                                          (< 0 (count stale)))        false
                                     true                             true)
           :attempt-count      (count rs)
           :stable-count       (count (:stable outcomes))
           :lost-count         (count (:lost outcomes))
           :lost               (sort (map :element (:lost outcomes)))
           :never-read-count   (count (:never-read outcomes))
           :never-read         (sort (map :element (:never-read outcomes)))
           :stale-count        (count stale)
           :stale              (sort (map :element stale))
           :worst-stale        worst-stale}
        points [0 0.5 0.95 0.99 1]
        m (if (seq stable-latencies)
            (assoc m :stable-latencies
                   (frequency-distribution points stable-latencies))
            m)
        m (if (seq lost-latencies)
            (assoc m :lost-latencies
                   (frequency-distribution points lost-latencies))
            m)]
    m))

(defn set-full
  "A more rigorous set analysis. We allow :add operations which add a single
  element, and :reads which return all elements present at that time. For each
  element, we construct a timeline like so:

      [nonexistent] ... [created] ... [present] ... [absent] ... [present] ...

  For each element:

  The *add* is the operation which added that element.

  The *known time* is the completion time of the add, or first read, whichever
  is earlier.

  The *stable time* is the time after which every read which begins observes
  the element. If every read beginning after the add time observes
  the element, the stable time is the add time. If the final read fails to
  observe the element, the stable time is nil.

  A *stable element* is one which has a stable time.

  The *lost time* is the time after which no operation observes that element.
  If the most final read observes the element, the lost time is nil.

  A *lost element* is one which has a lost time.

  An element can be either stable or lost, but not both.

  The *first read latency* is 0 if the first read invoked after the add time
  observes the element. If the element is never observed, it is nil. Otherwise,
  the first read delay is the time from the completion of the write to the
  invocation of the first read.

  The *stable latency* is the time between the add time and the stable time, or
  0, whichever is greater.

  Options are:

      :linearizable?    If true, we expect this set to be linearizable, and
                        stale reads result in an invalid result.

  Computes aggregate results:

      :valid?               False if there were any lost elements.
                            :unknown if there were no lost *or* stale elements;
                            e.g. if the test never inserted anything, every
                            insert crashed, etc. For :linearizable? tests,
                            false if there were lost *or* stale elements.
      :attempt-count        Number of attempted inserts
      :stable-count         Number of elements which had a time after which
                            they were always found
      :lost                 Elements which had a time after which
                            they were never found
      :lost-count           Number of lost elements
      :never-read           Elements where no read began after the time when
                            that element was known to have been inserted.
                            Includes elements which were never known to have
                            been inserted.
      :never-read-count     Number of elements never read
      :stale                Elements which failed to appear in a read beginning
                            after we knew the operation completed.
      :stale-count          Number of stale elements.
      :worst-stale          Detailed description of stale elements with the
                            highest stable latencies; e.g. which ones took the
                            longest to show up.
      :stable-latencies     Map of quantiles to latencies, in milliseconds, it
                            took for elements to become stable. 0 indicates the
                            element was linearizable.
      :lost-latencies       Map of quantiles to latencies, in milliseconds, it
                            took for elements to become lost. 0 indicates the
                            element was known to be inserted, but never
                            observed."
  ([]
   (set-full {:linearizable? false}))
  ([checker-opts]
  (reify Checker
    (check [this test history opts]
      ; Build up a map of elements to element states. We track the current set
      ; of ongoing reads as well, so we can map completions back to
      ; invocations. Finally we track a map of duplicates: elements to maximum
      ; multiplicities for that element in any given read.
      (let [[elements reads dups]
            (->> history
                 (r/filter (comp number? :process)) ; Ignore the nemesis
                 (reduce (fn red [[elements reads dups] op]
                           (let [v (:value op)
                                 p (:process op)]
                             (condp = (:f op)
                               :add
                               (if (= :invoke (:type op))
                                 ; Track a new element
                                 [(assoc elements v (set-full-element op))
                                  reads dups]
                                 ; Oh good, it completed
                                 [(update elements v set-full-add op)
                                  reads dups])

                               :read
                               (condp = (:type op)
                                 :invoke [elements (assoc reads p op) dups]
                                 :fail   [elements (dissoc reads p op) dups]
                                 :info   [elements reads dups]
                                 :ok
                                 (do ; We read stuff! Update every element
                                     (let [inv (get reads (:process op))
                                           ; Find duplicates
                                           dups' (->> (frequencies v)
                                                      (reduce (fn [m [k v]]
                                                                (if (< v 1)
                                                                  (assoc m k v)
                                                                  m))
                                                              (sorted-map))
                                                      (merge-with max dups))
                                           v   (into (sorted-set) v)]
                                       ; Process visibility of all elements
                                       [(map-kv (fn update-all [[element state]]
                                                  [element
                                                   (if (contains? v element)
                                                     (set-full-read-present
                                                       state inv op)
                                                     (set-full-read-absent
                                                       state inv op))])
                                                elements)
                                        reads
                                        dups']))))))
                         [{} {} (sorted-map)]))
            set-results (set-full-results checker-opts (vals elements))]
        (assoc set-results
               :valid?           (and (empty? dups) (:valid? set-results))
               :duplicated-count (count dups)
               :duplicated       dups))))))

(defn expand-queue-drain-ops
  "Takes a history. Looks for :drain operations with their value being a
  collection of queue elements, and expands them to a sequence of :dequeue
  invoke/complete pairs."
  [history]
  (reduce (fn [h' op]
            (cond ; Anything other than a drain op passes through
                  (not= :drain (:f op)) (conj h' op)

                  ; Skip drain invocations and failures
                  (op/invoke? op) h'
                  (op/fail? op)   h'

                  ; For successful drains, expand
                  (op/ok? op)
                  (into h' (mapcat (fn [element]
                                     [(assoc op
                                             :type  :invoke
                                             :f     :dequeue
                                             :value nil)
                                      (assoc op
                                             :type  :ok
                                             :f     :dequeue
                                             :value element)])
                                   (:value op)))

                  ; Anything else (e.g. crashed drains) is illegal
                  true
                  (throw (IllegalStateException.
                           (str "Not sure how to handle a crashed drain operation: "
                                (pr-str op))))))
          []
          history))

(defn total-queue
  "What goes in *must* come out. Verifies that every successful enqueue has a
  successful dequeue. Queues only obey this property if the history includes
  draining them completely. O(n)."
  []
  (reify Checker
    (check [this test history opts]
      (let [history  (expand-queue-drain-ops history)
            attempts (->> history
                          (r/filter op/invoke?)
                          (r/filter #(= :enqueue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            enqueues (->> history
                          (r/filter op/ok?)
                          (r/filter #(= :enqueue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            dequeues (->> history
                          (r/filter op/ok?)
                          (r/filter #(= :dequeue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            ; The OK set is every dequeue which we attempted.
            ok         (multiset/intersect dequeues attempts)

            ; Unexpected records are those we *never* tried to enqueue. Maybe
            ; leftovers from some earlier state. Definitely don't want your
            ; queue emitting records from nowhere!
            unexpected (->> dequeues
                            (remove (core/set (keys (multiset/multiplicities
                                                 attempts))))
                            (into (multiset/multiset)))

            ; Duplicate records are those which were dequeued more times than
            ; they could have been enqueued; but were attempted at least once.
            duplicated (-> dequeues
                           (multiset/minus attempts)
                           (multiset/minus unexpected))

            ; lost records are ones which we definitely enqueued but never
            ; came out.
            lost       (multiset/minus enqueues dequeues)

            ; Recovered records are dequeues where we didn't know if the enqueue
            ; suceeded or not, but an attempt took place.
            recovered  (multiset/minus ok enqueues)]

        {:valid?           (and (empty? lost) (empty? unexpected))
         :attempt-count    (count attempts)
         :acknowledged-count (count enqueues)
         :ok-count         (count ok)
         :unexpected-count (count unexpected)
         :duplicated-count (count duplicated)
         :lost-count       (count lost)
         :recovered-count  (count recovered)
         :lost             lost
         :unexpected       unexpected
         :duplicated       duplicated
         :recovered        recovered}))))

(defn unique-ids
  "Checks that a unique id generator actually emits unique IDs. Expects a
  history with :f :generate invocations matched by :ok responses with distinct
  IDs for their :values. IDs should be comparable. Returns

      {:valid?              Were all IDs unique?
       :attempted-count     Number of attempted ID generation calls
       :acknowledged-count  Number of IDs actually returned successfully
       :duplicated-count    Number of IDs which were not distinct
       :duplicated          A map of some duplicate IDs to the number of times
                            they appeared--not complete for perf reasons :D
       :range               [lowest-id highest-id]}"
  []
  (reify Checker
    (check [this test history opts]
      (let [attempted-count (->> history
                                 (filter op/invoke?)
                                 (filter #(= :generate (:f %)))
                                 count)
            acks     (->> history
                          (filter op/ok?)
                          (filter #(= :generate (:f %)))
                          (map :value))
            dups     (->> acks
                          (reduce (fn [counts id]
                               (assoc counts id
                                      (inc (get counts id 0))))
                             {})
                          (filter #(< 1 (val %)))
                          (into (sorted-map)))
            range    (reduce (fn [[lowest highest :as pair] id]
                               (cond (util/compare< id lowest)  [id highest]
                                     (util/compare< highest id) [lowest id]
                                     true           pair))
                             [(first acks) (first acks)]
                             acks)]
        {:valid?              (empty? dups)
         :attempted-count     attempted-count
         :acknowledged-count  (count acks)
         :duplicated-count    (count dups)
         :duplicated          (->> dups
                                   (sort-by val)
                                   (reverse)
                                   (take 48)
                                   (into (sorted-map)))
         :range               range}))))


(defn counter
  "A counter starts at zero; add operations should increment it by that much,
  and reads should return the present value. This checker validates that at
  each read, the value is greater than the sum of all :ok increments and
  attempted decrements, and lower than the sum of all attempted increments and
  :ok decrements.

  Returns a map:

  {:valid?              Whether the counter remained within bounds
   :reads               [[lower-bound read-value upper-bound] ...]
   :errors              [[lower-bound read-value upper-bound] ...]
  "
  []
  (reify Checker
    (check [this test history opts]
             ; Pre-process our history so failed adds do not get applied
      (loop [history         (->> history
                                  history/complete
                                  (remove :fails?)
                                  (remove op/fail?)
                                  seq)
             ; Current lower bound on counter.
             lower              0
             ; Upper bound on counter value.
             upper              0
             ; Map: process ID -> list of [lower upper] pairs for the pending read operation invoked by the process.
             ; Last pair in the list is most recent one and reflects the current counter possible range.
             ; List is cleared once read operation is completed.
             pending-reads      {}
             ; Completed reads: list of [lower val upper] tuples - one tuple for each successful read.
             ; Once read operation returns value val - we select first pair [lower upper] from pending-reads for which
             ; lower <= val <= upper, or just the first pair if no such pair is found (that means we've found an
             ; inconsistency, because val is out of any of possible ranges).
             ; TODO: probably we can use single union range instead of list of ranges and that will makes reads and
             ; errors reporting better.
             reads              []]
        (if (nil? history)
          ; We're done here
          (let [errors (remove (partial apply <=) reads)]
            {:valid?             (empty? errors)
             :reads              reads
             :errors             errors})
          ; But wait, there's more
          (let [op      (first history)
                process-id (:process op)
                history (next history)]
            (case [(:type op) (:f op)]
              [:invoke :read]
              (recur history lower upper
                     (assoc pending-reads process-id [[lower upper]])
                     reads)

              [:ok :read]
              (let [read-ranges (get pending-reads process-id)
                    v (:value op)
                    [l' u'] (first read-ranges)
                    read (or (some (fn [[l u]] (when (<= l v u) [l v u])) read-ranges)
                             [l' v u'])]
                (recur history lower upper
                       (dissoc pending-reads process-id)
                       (conj reads read)))

              [:invoke :add]
              (let [value (:value op)
                    [l' u'] (if (> value 0) [lower (+ upper value)] [(+ lower value) upper])]
                (recur history l' u'
                       (reduce-kv #(assoc %1 %2 (conj %3 [l' u'])) {} pending-reads)
                       reads))

              [:ok :add]
              (let [value (:value op)
                    [l' u'] (if (> value 0) [(+ lower value) upper] [lower (+ upper value)])]
                (recur history l' u'
                       (reduce-kv #(assoc %1 %2 (conj %3 [l' u'])) {} pending-reads)
                       reads))

              (recur history lower upper pending-reads reads))))))))

(defn latency-graph
  "Spits out graphs of latencies."
  []
  (reify Checker
    (check [_ test history opts]
      (perf/point-graph! test history opts)
      (perf/quantiles-graph! test history opts)
      {:valid? true})))

(defn rate-graph
  "Spits out graphs of throughput over time."
  []
  (reify Checker
    (check [_ test history opts]
      (perf/rate-graph! test history opts)
      {:valid? true})))

(defn perf
  "Assorted performance statistics"
  []
  (compose {:latency-graph (latency-graph)
            :rate-graph    (rate-graph)}))

(defn clock-plot
  "Plots clock offsets on all nodes"
  []
  (reify Checker
    (check [_ test history opts]
      (clock/plot! test history opts)
      {:valid? true})))
