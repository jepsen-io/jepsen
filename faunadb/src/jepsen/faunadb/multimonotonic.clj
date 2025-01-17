(ns jepsen.faunadb.multimonotonic
  "Similar to the monotonic test, this test takes an increment-only register
  and looks for cases where reads flow backwards. Unlike the monotonic test, we
  try to maximize performance (and our chances to observe nonmonotonicity) by
  sacrificing some generality: all updates to a given register happen in a
  single worker, and are performed as blind writes to avoid triggering OCC read
  locks which lower throughput"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.set :as set]
            [clojure.core.reducers :as r]
            [dom-top.core :as dt]
            [knossos.op :as op]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [util :as util]
                    [store :as store]]
            [jepsen.checker.perf :as perf]
            [gnuplot.core :as g]
            [jepsen.faunadb [query :as q]
                            [client :as f]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.time Instant)
           (java.time.temporal ChronoUnit)))

(def registers-name "registers")
(def registers (q/class registers-name))

(defn strip-time
  "Timestamps like 2018-12-05T22:15:09Z and 2018-12-05T22:15:09.143Z won't
  compare properly as strings; we strip off the trailing Z so we can sort
  them. Also converts instants to strings."
  [ts]
  (let [s (str ts)
        i (dec (count s))]
    (assert (= \Z (.charAt s i)))
    (subs s 0 i)))

(defn stripped-time->instant
  "Converts a stripped time string (without a Z) to an Instant."
  [s]
  (Instant/parse (str s "Z")))

(defn read-query
  "Query to read a given key."
  [k]
  (let [r (q/ref registers k)]
    (q/when (q/exists? r)
      (q/get r))))

(defn parse-instance
  "Turns an instance (possibly nil) into a map of {:ts \"...\", :value 4}"
  [instance]
  (when instance
    {;:value (+ (rand-int 5) (:value (:data instance)))
     :value (:value (:data instance))
     :ts    (:ts instance)}))

(defn sorted-zipmap-without-nil-vals
  "Like zipmap, but returns sorted maps, and skips nil values. Helpful for
  reading the maps of registers we generate."
	[keys vals]
	(loop [map (sorted-map)
				 ks (seq keys)
				 vs (seq vals)]
		(if (and ks vs)
			(recur (if (nil? (first vs))
               map
               (assoc map (first ks) (first vs)))
						 (next ks)
						 (next vs))
			map)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (f/with-retry
      (f/upsert-class! conn {:name registers-name})))

  (invoke! [this test op]
    (f/with-errors op #{:read}
      (assoc op
             :type :ok
             :value (case (:f op)
                      :write (do (->> (for [[k v] (:value op)]
                                        (f/upsert-by-ref
                                          (q/ref registers k)
                                          {:data {:value v}}))
                                      (f/query conn))
                                 (:value op))

                      :read
                      (let [ks (:value op)
                            [ts instances] (f/query conn
                                                    [(q/time "now")
                                                     (mapv read-query ks)])]
                        {:ts        (strip-time ts)
                         :registers (->> instances
                                         (map parse-instance)
                                         (sorted-zipmap-without-nil-vals ks))})))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn map-compare
  "A partial order comparator for maps. Returns -1 if m1 is less than m2, 0 if
  m1 and m2 are equivalent, and 1 if m1 is greater than m2. When m1 and m2 are
  incompatible, throws an ex-info describing the incompatibility.

  m1 and m2 are equivalent if, for all keys in common between m1 and m2, the
  values of those keys are the same in both maps.

  m1 is less than m2 if m1 and m2 are not equivalent, and for every key k
  present in both m1 and m2, (m1 k) is less than or equal to (m2 k).

  m1 and m2 are incompatible iff they are not equivalent and neither map is
  less than the other; e.g. there exists some pair of keys [k1 k2] in m1 and m2
  such that (m1 k1) < (m2 m1) and (m2 k2) < (m1 k2)."
  [m1 m2]
  ; We start off with a comparator value c of 0, for equivalent, and can shift
  ; to -1 or +1 if we observe m1 or m2 lower.
  (reduce (fn [c [k v1]]
            (if (not (contains? m2 k))
              ; This key isn't in common
              c

              ; OK, both m1 and m2 have this key in common; how about their
              ; values?
              (let [v2 (get m2 k)
                    c' (compare v1 v2)]
                (cond (neg? (* c c')) ; Conflicting signs!
                      (throw+ {:type :incomparable
                               :m1   m1
                               :m2   m2})

                      ; If one is zero, the other takes precedence
                      (zero? c)  c'
                      (zero? c') c

                      ; Both nonzero, same sign, either works
                      true c))))
          0
          m1))

(defn nonmonotonic-keys
  "Given a pair of state maps, returns a sequence of keys where the second
  map's value is lower than the first's. If no elements are nonmonotonic,
  returns nil."
  [m1 m2]
  (seq (keep (fn [[k v2]]
          (when-let [v1 (get m1 k)]
            (when (< v2 v1)
              k)))
        m2)))

(defn op->observation
  "Takes an operation and a key, and returns what that op observed for that
  key: a map of

      :ts       Timestamp associated with the observed instance state
      :read-ts  Timestamp the read executed at
      :value    Observed value
      :op-index Index of the read operation in the history"
  [op k]
  (let [value (:value op)
        register (get (:registers value) k)]
    {:read-ts   (:ts value)
     :ts        (:ts register)
     :value     (:value register)
     :op-index  (:index op)}))

(defn nonmonotonic-states
  "Takes a state function, which, given an operation, returns a state as
  observed by that operation; and a sequence of operations. Each state is a
  (possibly partial) map of keys to values, and, assuming each key's values are
  monotonically increasing, searches for states which violate that monotonicity
  assumption. Specifically, we traverse the sequence, inferring at every step a
  lower bound for the current value of each key, based on the maximum of all
  past read values for that key. If the current state under consideration has a
  key with a value *lower* than the upper bound on that key, we consider that a
  violation.

  We return a collection of error maps, each a map of

      :inferred-state    (restricted to keys observed by the nonmonotonic op)
      :observed-state    State map as seen by the nonmonotonic read
      :op                the read that was lower than the inferred state
      :errors            A map of keys to the reason that key was nonmonotonic

  Where each key error is a pair of maps from the previous observation of that
  key and the current observation of that key, like:

      [{:ts ..., :value ...}
       {:ts ..., :value ...}]"
  [state-fn history]
  (when-let [history (seq history)]
    (loop [history      history         ; Remaining ops to look at
           inferred     (sorted-map)    ; Map of keys to highest observations
           nonmonotonic (transient [])] ; Collection of errors
      (if history
        (let [op    (first history)
              state (state-fn op)]
          (recur (next history)
                 ; Update our inferred state map
                 (reduce (fn [inferred k]
                           (if (and (contains? inferred k)
                                    (< (get state k)
                                       (:value (get inferred k))))
                               ; We have an observation of this key already and
                               ; it's higher than this op's
                               inferred
                               (assoc inferred k (op->observation op k))))
                         inferred
                         (keys state))
                 ; And if we have any nonmonotonic keys here...
                 (if-let [nm-keys (nonmonotonic-keys
                                    (util/map-vals :value inferred)
                                    state)]
                   ; An error!
                   (conj! nonmonotonic
                          {:inferred (->> (keys state)
                                                (select-keys inferred)
                                                (util/map-vals :value))
                           :observed state
                           :op       op
                           :errors
                           (->> nm-keys
                                (map (fn [k]
                                       [(get inferred k)
                                        (op->observation op k)]))
                                (sorted-zipmap-without-nil-vals nm-keys))})
                   ; No errors
                   nonmonotonic)))
        ; Done
        (persistent! nonmonotonic)))))

(defn read-state
  "A function which, given a read op, returns the state map of register keys to
  values it observed."
  [op]
  (->> op :value :registers (util/map-vals :value)))

(defn read-compare
  "A comparator for reads; compares based on read state."
  [op1 op2]
  (map-compare (read-state op1) (read-state op2)))

(defn ts-order-checker
  "A checker which verifies that the timestamp order of events in the history
  is consistent with the observed values. We do this by ordering all reads in
  the history by their read timestamp, and playing forward a state machine,
  keeping track of the last observed value for each register. If we observe a
  read where some register's value is *lower* than the last observed state of
  that register, then we know (because each register is increment-only) that
  the system was internally inconsistent."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [errs (->> history
                      (filter op/ok?)
                      (filter (comp :ts :value))
                      (sort-by (comp :ts :value))
                      (nonmonotonic-states read-state))]
        {:valid? (empty? errs)
         :errors errs}))))

(defn read-skew-checker
  "A checker which searches for incidents of read skew. Because each register
  is increment-only, we know that there should never exist a pair of reads r1
  and r2, such that for two registers x and y, where both registers are
  observed by both reads, x_r1 < x_r2 and y_r1 > y_r2.

  This problem is equivalent to cycle detection: we have a set of partial
  orders <x, <y, ..., each of which relates states based on whether x increases
  or not. We're trying to determine whether these orders are *compatible*.

  Imagine an order <x as a graph over states, and likewise for <y, <z, etc.
  Take the union of these graphs. If all these orders are compatible, there
  should be no cycles in this graph.

  To do this, we take each key k, and find all values for k. In general, the
  ordering relation <k is the transitive closure, but for cycle detection, we
  don't actually need the full closure--we'll restrict ourselves to k=1's
  successors being those with k=2 (or, if there are no k=2, use k=3, etc). This
  gives us a set of directed edges over states for k; we union the graphs for
  all k together to obtain a graph of all relationships.

  Next, we apply Tarjan's algorithm for strongly connected components, which is
  linear in edges + vertices (which is why we don't work with the full
  transitive closure of <k). The existence of any strongly connected components
  containing more than one vertex implies a cycle in the graph, and that cycle
  will be within that component.

  This isn't suuuper ideal... the connected component could, I guess, be fairly
  large, and then it'd be hard to prove where the cycle lies. But this feels
  like an OK start."
  []
  (reify checker/Checker
    (check [this test history opts]
      (try+ (let [order (->> history
                             (r/filter op/ok?)
                             (r/filter (comp #{:read} :f))
                             (into []))]
              {:valid? true}
              )))))

(defn generator
  [opts]
  ; A map of worker threads to [key, last-written-value] pairs.
  (let [active-keys (atom {})
        writes (reify gen/Generator
                 (op [this test process]
                   (let [thread (gen/process->thread test process)
                         ; Just derive keys from process ids; that way they're
                         ; unique per thread, and we never have concurrent
                         ; updates on a key.
                         k'     process
                         ak (swap! active-keys
                                   (fn [ak]
                                     (let [[k v] (get ak thread)
                                           v' (if (= k k') (inc v) 0)]
                                       (assoc ak thread [k' v']))))
                         v' (second (get ak thread))]
                     {:type :invoke, :f :write, :value {k' v'}})))
        reads (reify gen/Generator
                (op [this test process]
                  {:type  :invoke,
                   :f     :read
                   :value (->> @active-keys
                               vals
                               (map first)
                               util/random-nonempty-subset)}))]
    (gen/reserve (/ (:concurrency opts) 2) writes
                 reads)))

(defn workload
  [opts]
  (let [n (count (:nodes opts))]
    {:client    (Client. nil)
     :generator (->> (generator opts)
                     ; (gen/stagger 1)
                     )
     :checker (checker/compose
                {:ts-order  (ts-order-checker)
                 :read-skew (read-skew-checker)})}))
