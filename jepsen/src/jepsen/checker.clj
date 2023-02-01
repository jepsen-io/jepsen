(ns jepsen.checker
  "Validates that a history is correct with respect to some model."
  (:refer-clojure :exclude [set])
  (:require [clojure [core :as c]
                     [pprint :refer [pprint]]
                     [set :as set]
                     [stacktrace :as trace]
                     [string :as str]]
            [clojure.core.reducers :as r]
            [clojure.java [io :as io]
                          [shell :refer [sh]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :as dt :refer [loopr]]
            [potemkin :refer [definterface+]]
            [jepsen [history :as h]
                    [store :as store]
                    [util :as util :refer [meh fraction map-kv]]]
            [jepsen.checker [perf :as perf]
                            [clock :as clock]]
            [jepsen.history.fold :as f]
            [multiset.core :as multiset]
            [gnuplot.core :as g]
            [knossos [model :as model]
                     [op :as op]
                     [competition :as competition]
                     [linear :as linear]
                     [wgl :as wgl]
                     [history :as history]]
            [knossos.linear.report :as linear.report]
            [slingshot.slingshot :refer [try+ throw+]]
            [tesser.core :as t])
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
                         output files should be written. Defaults to nil."))
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

(defn unhandled-exceptions
  "Returns information about unhandled exceptions: a sequence of maps sorted in
  descending frequency order, each with:

      :class    The class of the exception thrown
      :count    How many of this exception we observed
      :example  An example operation"
  []
  (reify Checker
    (check [this test history opts]
      (let [exes (->> (t/filter h/info?)
                      (t/filter :exception)
                      ; TODO: do we want the first or last :via?
                      (t/group-by (comp :type first :via :exception))
                      (t/into [])
                      (h/tesser history)
                      vals
                      (sort-by count)
                      reverse
                      (map (fn [ops]
                             (let [op (first ops)
                                   e  (:exception op)]
                               {:count (count ops)
                                :class (-> e :via first :type)
                                :example op}))))]
        (if (seq exes)
          {:valid?      true
           :exceptions  exes}
          {:valid? true})))))

(def stats-fold
  "Helper for computing stats over a history or filtered history."
  (f/loopf {:name :stats}
           ; Reduce
           ([^long oks   0
             ^long infos 0
             ^long fails 0]
            [{:keys [type]}]
            (case type
              :ok   (recur (inc oks) infos fails)
              :info (recur oks (inc infos) fails)
              :fail (recur oks infos (inc fails))))
           ; Combine
           ([oks 0, infos 0, fails 0]
            [[oks' infos' fails']]
            (recur (+ oks oks')
                   (+ infos infos')
                   (+ fails fails'))
            {:valid?     (pos? oks)
             :count      (+ oks fails infos)
             :ok-count   oks
             :fail-count fails
             :info-count infos})))

(defn stats
  "Computes basic statistics about success and failure rates, both overall and
  broken down by :f. Results are valid only if every :f has at some :ok
  operations; otherwise they're :unknown."
  []
  (reify Checker
    (check [this test history opts]
      (let [history (->> history
                         (h/remove h/invoke?)
                         h/client-ops)
            {:keys [all by-f]}
            (->> (t/fuse {:all (t/fold stats-fold)
                          :by-f (->> (t/group-by :f)
                                     (t/fold stats-fold))})
                 (h/tesser history))]
        (assoc all
               :by-f    (into (sorted-map) by-f)
               :valid?  (merge-valid (map :valid? (vals by-f))))))))

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
                       (h/filter (fn select [op]
                                   (condp = (:f op)
                                     :enqueue (h/invoke? op)
                                     :dequeue (h/ok? op)
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
      ; This would be more efficient as a single fused fold, but I'm doing it
      ; this way to exercise/demonstrate the task system. Will replace later
      ; once profiling shows it as a bottleneck.
      (let [attempts (h/task history attempts []
                             (->> (t/filter h/invoke?)
                                  (t/filter (h/has-f? :add))
                                  (t/map :value)
                                  (t/set)
                                  (h/tesser history)))
            adds (h/task history adds []
                         (->> (t/filter h/ok?)
                              (t/filter (h/has-f? :add))
                              (t/map :value)
                              (t/set)
                              (h/tesser history)))
            final-read (h/task history final-reads []
                               (->> (t/filter (h/has-f? :read))
                                    (t/filter h/ok?)
                                    (t/map :value)
                                    (t/last)
                                    (h/tesser history)))
            attempts   @attempts
            adds       @adds
            final-read @final-read]
        (if-not final-read
          {:valid? :unknown
           :error  "Set was never read"}

          (let [final-read (c/set final-read)

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
      (let [n (c/count sorted)
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
      ; Build up a map of elements to element states. Finally we track a map of
      ; duplicates: elements to maximum multiplicities for that element in any
      ; given read.
      (let [[elements dups]
            (->> history
                 h/client-ops
                 (reduce (fn red [[elements dups] op]
                           (let [v (:value op)
                                 p (:process op)]
                             (condp = (:f op)
                               :add
                               (if (= :invoke (:type op))
                                 ; Track a new element
                                 [(assoc elements v (set-full-element op)) dups]
                                 ; Oh good, it completed
                                 [(update elements v set-full-add op) dups])

                               :read
                               (if-not (h/ok? op)
                                 ; Nothing doing
                                 [elements dups]
                                 ; We read stuff! Update every element
                                 (let [inv (h/invocation history op)
                                       ; Find duplicates
                                       dups' (->> (frequencies v)
                                                  (reduce (fn [m [k v]]
                                                            (if (< v 1)
                                                              (assoc m k v)
                                                              m))
                                                          (sorted-map))
                                                  (merge-with max dups))
                                       v   (c/set v)]
                                   ; Process visibility of all elements
                                   [(map-kv (fn update-all [[element state]]
                                              [element
                                               (if (contains? v element)
                                                 (set-full-read-present
                                                   state inv op)
                                                 (set-full-read-absent
                                                   state inv op))])
                                            elements)
                                    dups'])))))
                         [{} {}]))
            set-results (set-full-results checker-opts
                                          (mapv val (sort elements)))]
        (assoc set-results
               :valid?           (and (empty? dups) (:valid? set-results))
               :duplicated-count (count dups)
               :duplicated       dups))))))

(defn expand-queue-drain-ops
  "A Tesser fold which looks for :drain operations with their value being a
  collection of queue elements, and expands them to a sequence of :dequeue
  invoke/complete pairs."
  []
  (t/mapcat (fn expand [op]
              (cond ; Pass through anything other than a :drain
                    (not= :drain (:f op)) [op]

                    ; Skip drain invokes/fails
                    (h/invoke? op) nil
                    (h/fail? op) nil

                    ; Expand successful drains
                    (h/ok? op)
                    (mapcat (fn [element]
                              [(assoc op
                                      :index -1
                                      :type  :invoke
                                      :f     :dequeue
                                      :value nil)
                               (assoc op
                                      :index -1
                                      :type  :ok
                                      :f     :dequeue
                                      :value element)])
                            (:value op))

                    ; Anything else (e.g. crashed drains) is illegal
                    true
                    (throw (IllegalStateException.
                             (str "Not sure how to handle a crashed drain operation: "
                                  (pr-str op))))))))

(defn total-queue
  "What goes in *must* come out. Verifies that every successful enqueue has a
  successful dequeue. Queues only obey this property if the history includes
  draining them completely. O(n)."
  []
  (reify Checker
    (check [this test history opts]
      (let [{:keys [attempts enqueues dequeues]}
            (->> (expand-queue-drain-ops)
                 (t/fuse
                   {:attempts (->> (t/filter (h/has-f? :enqueue))
                                   (t/filter h/invoke?)
                                   (t/map :value)
                                   (t/into (multiset/multiset)))
                    :enqueues (->> (t/filter (h/has-f? :enqueue))
                                   (t/filter h/ok?)
                                   (t/map :value)
                                   (t/into (multiset/multiset)))
                    :dequeues (->> (t/filter (h/has-f? :dequeue))
                                   (t/filter h/ok?)
                                   (t/map :value)
                                   (t/into (multiset/multiset)))})
                 (h/tesser history))

            ; The OK set is every dequeue which we attempted.
            ok         (multiset/intersect dequeues attempts)

            ; Unexpected records are those we *never* tried to enqueue. Maybe
            ; leftovers from some earlier state. Definitely don't want your
            ; queue emitting records from nowhere!
            unexpected (->> dequeues
                            (remove (c/set (keys (multiset/multiplicities
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
      (let [{:keys [attempted-count acks]}
            (->> (t/filter (h/has-f? :generate))
                 (t/fuse {:attempted-count (->> (t/filter h/invoke?)
                                                (t/count))
                          :acks (->> (t/filter h/ok?)
                                     (t/map :value)
                                     (t/fuse {:count (t/count)
                                              :freqs (t/frequencies)
                                              :range (t/range)}))})
                 (h/tesser history))
            dups (->> acks :freqs
                      (r/filter #(< 1 (val %)))
                      (into (sorted-map)))]
        {:valid?              (empty? dups)
         :attempted-count     attempted-count
         :acknowledged-count  (:count acks)
         :duplicated-count    (count dups)
         :duplicated          (->> dups
                                   (sort-by val)
                                   (reverse)
                                   (take 48)
                                   (into (sorted-map)))
         :range               (:range acks)}))))

(defn counter
  "A counter starts at zero; add operations should increment it by that much,
  and reads should return the present value. This checker validates that at
  each read, the value is greater than the sum of all :ok increments, and lower
  than the sum of all attempted increments.

  Note that this counter verifier assumes the value monotonically increases:
  decrements are not allowed.

  Returns a map:

    :valid?              Whether the counter remained within bounds
    :reads               [[lower-bound read-value upper-bound] ...]
    :errors              [[lower-bound read-value upper-bound] ...]

  ; Not implemented, but might be nice:

    :max-absolute-error The [lower read upper] where read falls furthest outside
    :max-relative-error Same, but with error computed as a fraction of the mean}
  "
  []
  (reify Checker
    (check [this test history opts]
      ; pre-process our history so failed adds do not get applied
      (loopr [lower              0   ; Current lower bound on counter
              upper              0   ; Upper bound on counter value
              pending-reads      {}  ; Process ID -> [lower read-val]
              reads              []] ; Completed [lower val upper]s
             ; If this is slow, maybe try :via :reduce?
             [{:keys [process type f process value] :as op} history]
             (case [type f]
               [:invoke :read]
               ; What value will this read?
               (if-let [completion (h/completion history op)]
                 (do (if (h/ok? completion)
                       ; We're going to read something
                       (recur lower upper
                              (assoc pending-reads process
                                     [lower (:value completion)])
                              reads)
                       ; Won't read anything
                       (recur lower upper pending-reads reads)))
                 ; Doesn't finish at all
                 (recur lower upper pending-reads reads))

               [:ok :read]
               (let [r (get pending-reads process)]
                 (recur lower upper
                        (dissoc pending-reads process)
                        (conj reads (conj r upper))))

               [:invoke :add]
               (do (assert (not (neg? value)))
                   ; Look forward to find out if we'll succeed
                   (if-let [completion (h/completion history op)]
                     (if (h/fail? completion)
                       ; Won't complete
                       (recur lower upper pending-reads reads)
                       ; Will complete
                       (recur lower (+ upper value) pending-reads reads))
                     ; Not sure
                     (recur lower (+ upper value) pending-reads reads)))

               [:ok :add]
               (recur (+ lower value) upper pending-reads reads)

               (recur lower upper pending-reads reads))
                 (let [errors (remove (partial apply <=) reads)]
                   {:valid?             (empty? errors)
                    :reads              reads
                    :errors             errors})))))

(defn latency-graph
  "Spits out graphs of latencies. Checker options take precedence over
  those passed in with this constructor."
  ([]
   (latency-graph {}))
  ([opts]
   (reify Checker
     (check [_ test history c-opts]
       (let [o (merge opts c-opts)]
         (perf/point-graph!     test history o)
         (perf/quantiles-graph! test history o)
         {:valid? true})))))

(defn rate-graph
  "Spits out graphs of throughput over time. Checker options take precedence
  over those passed in with this constructor."
  ([]
   (rate-graph {}))
  ([opts]
   (reify Checker
     (check [_ test history c-opts]
       (let [o (merge opts c-opts)]
         (perf/rate-graph! test history o)
         {:valid? true})))))

(defn perf
  "Composes various performance statistics. Checker options take precedence over
  those passed in with this constructor."
  ([]
   (perf {}))
  ([opts]
   (compose {:latency-graph (latency-graph opts)
             :rate-graph    (rate-graph opts)})))

(defn clock-plot
  "Plots clock offsets on all nodes"
  []
  (reify Checker
    (check [_ test history opts]
      (clock/plot! test history opts)
      {:valid? true})))

(defn log-file-pattern
  "Takes a PCRE regular expression pattern (as a Pattern or string) and a
  filename. Checks the store directory for this test, and in each node
  directory (e.g. n1), examines the given file to see if it contains instances
  of the pattern. Returns :valid? true if no instances are found, and :valid?
  false otherwise, along with a :count of the number of matches, and a :matches
  list of maps, each with the node and matching string from the file.

    (log-file-pattern-checker #\"panic: (\\w+)$\" \"db.log\")

    {:valid? false
     :count  5
     :matches {:node \"n1\"
               :line \"panic: invariant violation\" \"invariant violation\"
               ...]}}"
  [pattern filename]
  (reify Checker
    (check [this test history opts]
      (let [matches
            (->> (:nodes test)
                 (pmap (fn search [node]
                         (let [{:keys [exit out err]}
                               (->> (store/path test node filename)
                                    .getCanonicalPath
                                    (sh "grep" "--text" "-P" (str pattern)))]
                           (case (long exit)
                             0 (->> out
                                    str/split-lines
                                    (map (fn [line]
                                           {:node node, :line line})))
                             ; No match
                             1 nil
                             2 (condp re-find err
                                 #"No such file" nil
                                 (throw+ {:type :grep-error
                                        :exit exit
                                        :cmd (str "grep -P " pattern)
                                        :err err
                                        :out out}))))))
                 (mapcat identity))]
        {:valid? (empty? matches)
         :count  (count matches)
         :matches matches}))))
