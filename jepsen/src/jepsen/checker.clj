(ns jepsen.checker
  "Validates that a history is correct with respect to some model."
  (:refer-clojure :exclude [set])
  (:require [clojure.stacktrace :as trace]
            [clojure.core :as core]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [jepsen.util :as util]
            [multiset.core :as multiset]
            [knossos.core :as knossos]))

(defprotocol Checker
  (check [checker test model history]
         "Verify the history is correct. Returns a map like

         {:valid? true}

         or

         {:valid?    false
          :failed-at [details of specific operations]}

         and maybe there can be some stats about what fraction of requests
         were corrupt, etc."))

(defn check-safe
  "Like check, but wraps exceptions up and returns them as a map like

  {:valid? nil :error \"...\"}"
  [checker test model history]
  (try (check checker test model history)
       (catch Throwable t
         {:valid? false
          :error (with-out-str (trace/print-cause-trace t))})))

(def unbridled-optimism
  "Everything is awesoooommmmme!"
  (reify Checker
    (check [this test model history] {:valid? true})))

(def linearizable
  "Validates linearizability with Knossos."
  (reify Checker
    (check [this test model history]
      (knossos/analysis model history))))

(def queue
  "Every dequeue must come from somewhere. Validates queue operations by
  assuming every non-failing enqueue succeeded, and only OK dequeues succeeded,
  then reducing the model with that history. Every subhistory of every queue
  should obey this property. Should probably be used with an unordered queue
  model, because we don't look for alternate orderings. O(n)."
  (reify Checker
    (check [this test model history]
      (let [final (->> history
                       (r/filter (fn select [op]
                                   (condp = (:f op)
                                     :enqueue (knossos/invoke? op)
                                     :dequeue (knossos/ok? op)
                                     false)))
                                 (reduce knossos/step model))]
        (if (knossos/inconsistent? final)
          {:valid? false
           :error  (:msg final)}
          {:valid?      true
           :final-queue final})))))

(def set
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted."
  (reify Checker
    (check [this test model history]
      (let [attempts (->> history
                          (r/filter knossos/invoke?)
                          (r/filter #(= :add (:f %)))
                          (r/map :value)
                          (into #{}))
            adds (->> history
                      (r/filter knossos/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            final-read (->> history
                          (r/filter knossos/ok?)
                          (r/filter #(= :read (:f %)))
                          (r/map :value)
                          (reduce (fn [_ x] x) nil))]
        (if-not final-read
          {:valid? false
           :error  "Set was never read"})

        (let [; The OK set is every read value which we tried to add
              ok          (set/intersection final-read attempts)

              ; Unexpected records are those we *never* attempted.
              unexpected  (set/difference final-read attempts)

              ; Lost records are those we definitely added but weren't read
              lost        (set/difference adds final-read)

              ; Recovered records are those where we didn't know if the add
              ; succeeded or not, but we found them in the final set.
              recovered   (set/difference ok adds)]

          {:valid?          (and (empty? lost) (empty? unexpected))
           :ok              (util/integer-interval-set-str ok)
           :lost            (util/integer-interval-set-str lost)
           :unexpected      (util/integer-interval-set-str unexpected)
           :recovered       (util/integer-interval-set-str recovered)
           :ok-frac         (util/fraction (count ok) (count attempts))
           :unexpected-frac (util/fraction (count unexpected) (count attempts))
           :lost-frac       (util/fraction (count lost) (count attempts))
           :recovered-frac  (util/fraction (count recovered) (count attempts))})))))

(defn fraction
  "a/b, but if b is zero, returns unity."
  [a b]
  (if (zero? b)
           1
           (/ a b)))

(def total-queue
  "What goes in *must* come out. Verifies that every successful enqueue has a
  successful dequeue. Queues only obey this property if the history includes
  draining them completely. O(n)."
  (reify Checker
    (check [this test model history]
      (let [attempts (->> history
                          (r/filter knossos/invoke?)
                          (r/filter #(= :enqueue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            enqueues (->> history
                          (r/filter knossos/ok?)
                          (r/filter #(= :enqueue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            dequeues (->> history
                          (r/filter knossos/ok?)
                          (r/filter #(= :dequeue (:f %)))
                          (r/map :value)
                          (into (multiset/multiset)))
            ; The OK set is every dequeue which we attempted.
            ok         (multiset/intersect dequeues attempts)

            ; Unexpected records are those we *never* attempted. Maybe
            ; duplicates, maybe leftovers from some earlier state. Definitely
            ; don't want your queue emitting records from nowhere!
            unexpected (multiset/minus dequeues attempts)

            ; lost records are ones which we definitely enqueued but never
            ; came out.
            lost       (multiset/minus enqueues dequeues)

            ; Recovered records are dequeues where we didn't know if the enqueue
            ; suceeded or not, but an attempt took place.
            recovered  (multiset/minus ok enqueues)]

        {:valid?          (and (empty? lost) (empty? unexpected))
         :lost            lost
         :unexpected      unexpected
         :recovered       recovered
         :ok-frac         (util/fraction (count ok)         (count attempts))
         :unexpected-frac (util/fraction (count unexpected) (count attempts))
         :lost-frac       (util/fraction (count lost)       (count attempts))
         :recovered-frac  (util/fraction (count recovered)  (count attempts))}))))

(defn compose
  "Takes a map of names to checkers, and returns a checker which runs each
  check (possibly in parallel) and returns a map of names to results; plus a
  top-level :valid? key which is true iff every checker considered the history
  valid."
  [checker-map]
  (reify Checker
    (check [this test model history]
      (let [results (->> checker-map
                         (pmap (fn [[k checker]]
                                 [k (check checker test model history)]))
                         (into {}))]
        (assoc results :valid? (every? :valid? (vals results)))))))
