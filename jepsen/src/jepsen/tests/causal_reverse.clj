(ns jepsen.tests.causal-reverse
  "Checks for a strict serializability anomaly in which T1 < T2, but T2 is
  visible without T1.

  We perform concurrent blind inserts across n keys, and meanwhile, perform
  reads of n keys in a transaction. To verify, we replay the history,
  tracking the writes which were known to have completed before the invocation
  of any write w_i. If w_i is visible, and some w_j < w_i is *not* visible,
  we've found a violation of strict serializability.

  Splits keys up onto different tables to make sure they fall in different
  shard ranges"
  (:require [jepsen [checker :as checker]
                    [generator :as gen]
                    [history :as h]
                    [independent :as independent]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]))

(defn graph
  "Takes a history and returns a first-order write precedence graph."
  [history]
  (loop [completed  (sorted-set)
         expected   {}
         [op & more :as history] history]
    (cond
      ;; Done
      (not (seq history))
      expected

      ;; We know this value is definitely written
      (= :write (:f op))
      (cond
        ;; Write is beginning; record precedence
        (h/invoke? op)
        (recur completed
               (assoc expected (:value op) completed)
               more)

        ;; Write is completing; we can now expect to see
        ;; it
        (h/ok? op)
        (recur (conj completed (:value op))
               expected more)

        ;; Not a write, ignore and continue
        true (recur completed expected more))
      true (recur completed expected more))))

(defn errors
  "Takes a history and an expected graph of write precedence, returning ops that
  violate the expected write order."
  [history expected]
  (let [h (->> history
               (h/filter (h/has-f? :read))
               (h/filter h/ok?))
        f (fn [errors op]
            (let [seen         (:value op)
                  our-expected (->> seen
                                    (map expected)
                                    (reduce set/union))
                  missing (set/difference our-expected
                                          seen)]
              (if (empty? missing)
                errors
                (conj errors
                      (-> op
                          (dissoc :value)
                          (assoc :missing missing)
                          (assoc :expected-count
                                 (count our-expected)))))))]
    (reduce f [] h)))

(defn checker
  "Takes a history of writes and reads. Verifies that subquent writes do not
  appear without prior acknowledged writes."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [expected (graph history)
            errors   (errors history expected)]
        {:valid? (empty? errors)
         :errors errors}))))

(defn r []  {:type :invoke, :f :read, :value nil})
(defn w [k] {:type :invoke, :f :write, :value k})

(defn workload
  "A package of a generator and checker. Options:

    :nodes            A set of nodes you're going to operate on. We only care
                      about the count, so we can figure out how many workers
                      to use per key.
    :per-key-limit    Maximum number of ops per key. Default 500."
  [opts]
  {:checker   (checker/compose
               {:perf       (checker/perf)
                :sequential (independent/checker (checker))})
   :generator (let [n       (count (:nodes opts))
                    reads   {:f :read}
                    writes  (map (fn [x] {:f :write, :value x}) (range))]
                  (independent/concurrent-generator
                    n
                    (range)
                    (fn [k]
                      ; TODO: I'm not entirely sure this is the same--I
                      ; thiiiink the original generator didn't actually mean to
                      ; re-use the same write generator for each distinct key,
                      ; but if it *did* and relied on that behavior, this will
                      ; break.
                      (->> (gen/mix [reads writes])
                           (gen/stagger 1/100)
                           (gen/limit (:per-key-limit opts 500))))))})
