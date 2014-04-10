(ns jepsen.checker
  "Validates that a history is correct with respect to some model."
  (:require [clojure.stacktrace :as trace]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
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
         :ok-frac         (try (/ (count ok)         (count attempts))
                               (catch ArithmeticException _ 1))
         :unexpected-frac (try (/ (count unexpected) (count attempts))
                               (catch ArithmeticException _ 1))
         :lost-frac       (try (/ (count lost)    (count attempts))
                               (catch ArithmeticException _ 1))
         :recovered-frac  (try (/ (count recovered)  (count attempts))
                               (catch ArithmeticException _ 1))}))))

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
