(ns jepsen.independent
  "Some tests are expensive to check--for instance, linearizability--which
  requires we verify only short histories. But if histories are short, we may
  not be able to sample often or long enough to reveal concurrency errors. This
  namespace supports splitting a test into independent components--for example
  taking a test of a single register and lifting it to a *map* of keys to
  registers."
  (:require [jepsen.checker :refer [Checker]]
            [jepsen.generator :as gen :refer [Generator]]))

(defn sequential-generator
  "Takes a sequence of keys (k1 k2 ...), and a function (fgen k) which, when
  called with a key, yields a generator. Returns a generator which starts with
  the first key k1 and constructs a generator gen1 from (fgen k1). Returns
  elements from gen1 until it is exhausted, then moves to k2.

  The generator wraps each :value in the operations it generates. Let (:value
  (op gen1)) be v; then the generator we construct yields the kv tuple [k1 v].

  fgen must be pure and idempotent."
  [keys fgen]
  (let [state (atom {:keys keys
                     :gen  (when-let [k1 (first keys)]
                             (fgen k1))})]
    (reify Generator
      (op [this test process]
        (let [{:keys [keys gen] :as s} @state]
          (when-let [k (first keys)]
            (if-let [op (gen/op gen test process)]
              (assoc op :value (clojure.lang.MapEntry. k (:value op)))
              ; Generator exhausted
              (when-let [keys' (next keys)]
                (compare-and-set! state s {:keys keys'
                                           :gen  (fgen (first keys'))})
                (recur test process)))))))))
