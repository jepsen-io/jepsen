(ns jepsen.checker
  "Validates that a history is correct with respect to some model."
  (:refer-clojure :exclude [set])
  (:require [clojure.stacktrace :as trace]
            [clojure.core :as core]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.util :as util :refer [meh fraction]]
            [jepsen.store :as store]
            [jepsen.checker.perf :as perf]
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
  (check [checker test model history opts]
         "Verify the history is correct. Returns a map like

         {:valid? true}

         or

         {:valid?       false
          :some-details ...
          :failed-at    [details of specific operations]}

         Opts is a map of options controlling checker execution. Keys include:

         :subdirectory - A directory within this test's store directory where
                         output files should be written. Defaults to nil."))

(defn check-safe
  "Like check, but wraps exceptions up and returns them as a map like

  {:valid? :unknown :error \"...\"}"
  ([checker test model history]
   (check-safe checker test model history {}))
  ([checker test model history opts]
   (try (check checker test model history opts)
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
    (check [this test model history opts]
      (let [results (->> checker-map
                         (pmap (fn [[k checker]]
                                 [k (check-safe checker test model history opts)]))
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
      (check [this test model history opts]
        (try (.acquire sem)
             (check checker test model history opts)
             (finally
               (.release sem)))))))

(defn unbridled-optimism
  "Everything is awesoooommmmme!"
  []
  (reify Checker
    (check [this test model history opts] {:valid? true})))

(defn linearizable
  "Validates linearizability with Knossos. Defaults to the competition checker,
  but can be controlled by passing either :linear or :wgl."
  ([]
   (linearizable :competition))
  ([algorithm]
     (reify Checker
       (check [this test model history opts]
         (let [a ((case algorithm
                    :competition  competition/analysis
                    :linear       linear/analysis
                    :wgl          wgl/analysis)
                  model history)]
           (when-not (:valid? a)
             (try
               ; Renderer can't handle really broad concurrencies yet
               (linear.report/render-analysis!
                 history a (.getCanonicalPath
                             (store/path! test (:subdirectory opts)
                                          "linear.svg")))
               (catch Throwable e
                 (warn e "Error rendering linearizability analysis"))))
           ; Writing these can take *hours* so we truncate
           (assoc a
                  :final-paths (take 10 (:final-paths a))
                  :configs     (take 10 (:configs a))))))))

(defn queue
  "Every dequeue must come from somewhere. Validates queue operations by
  assuming every non-failing enqueue succeeded, and only OK dequeues succeeded,
  then reducing the model with that history. Every subhistory of every queue
  should obey this property. Should probably be used with an unordered queue
  model, because we don't look for alternate orderings. O(n)."
  []
  (reify Checker
    (check [this test model history opts]
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
    (check [this test model history opts]
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
    (check [this test model history opts]
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
    (check [this test model history opts]
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
  each read, the value is at greater than the sum of all :ok increments and
  attempted decrements, and lower than the sum of all attempted increments and
  :ok decrements.

  Since we know a :fail increment did not occur, we should decrement the
  counter by the appropriate amount.

  TODO: filter out failed operations in an initial pass.

  Returns a map:

  {:valid?              Whether the counter remained within bounds
   :reads               [[lower-bound read-value upper-bound] ...]
   :errors              [[lower-bound read-value upper-bound] ...]
  "
  []
  (reify Checker
    (check [this test model history opts]
      (loop [history            (seq (history/complete history))
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

              [:fail :add]
              (let [value (:value op)
                    [l' u'] (if (> value 0) [lower (- upper value)] [(- lower value) upper])]
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
    (check [_ test model history opts]
      (perf/point-graph! test history opts)
      (perf/quantiles-graph! test history opts)
      {:valid? true})))

(defn rate-graph
  "Spits out graphs of throughput over time."
  []
  (reify Checker
    (check [_ test model history opts]
      (perf/rate-graph! test history opts)
      {:valid? true})))

(defn perf
  "Assorted performance statistics"
  []
  (compose {:latency-graph (latency-graph)
            :rate-graph    (rate-graph)}))
