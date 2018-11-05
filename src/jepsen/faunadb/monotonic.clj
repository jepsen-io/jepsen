(ns jepsen.faunadb.monotonic
  "Verifies that clients observe monotonic state and timestamps when performing
  current reads, and that reads of past timestamps observe monotonic state.

  For our monotonic state, we'll use a register, implemented as an instance
  with a single value. That register will be incremented by `inc` calls,
  starting at 0.

  {:type :invoke, :f :inc, :value nil}

  which returns

  {:type :invoke, :f inc, :value [ts, v]}

  Meaning that we set the value to v at time ts.

  Meanwhile, we'll execute reads like:

  {:type :invoke, :f :read, :value [ts, nil]}

  which means we should read the register at time `ts`, returning

  {:type :ok, :f :read, :value [ts, v]}.

  If the timestamp is nil, we read at the current time, and return the
  timestamp we executed at."
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.set :as set]
            [clojure.core.reducers :as r]
            [dom-top.core :as dt]
            [knossos.op :as op]
            [jepsen [client :as client]
                    [checker :as checker]
                    [fauna :as fauna]
                    [generator :as gen]
                    [independent :as independent]
                    [util :as util]
                    [store :as store]]
            [jepsen.checker.perf :as perf]
            [gnuplot.core :as g]
            [jepsen.faunadb [query :as q]
                            [client :as f]]))

(def registers-name "registers")
(def registers (q/class registers-name))

(def k 0)

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (f/with-retry
      (f/query conn (f/upsert-class {:name registers-name}))))

  (invoke! [this test op]
    (f/with-errors op #{:read-at :read}
      (let [v   (:value op)
            r   (q/ref registers k)
            res (case (:f op)
                  :inc
                  (f/query conn
                           [(q/time "now")
                            (q/if (q/exists? r)
                              ; Record exists, increment
                              (q/let [v  (q/select ["data" "value"] (q/get r))
                                      v' (q/+ v 1)]
                                (q/update r {:data {:value v'}})
                                v')
                              ; Record doesn't exist, init to 1
                              (q/do (q/create r {:data {:value 1}})
                                    1))])

                  :read    (f/query conn
                                    [(q/time "now")
                                     (q/if (q/exists? r)
                                       (q/select ["data" "value"] (q/get r))
                                       0)])

                  :read-at (let [ts (or (first v)
                                        (f/jitter-time (f/now conn)))]
                               (f/query conn
                                        [ts
                                         (q/at ts
                                               (if (q/exists? r)
                                                 (q/select ["data" "value"]
                                                           (q/get r))
                                                 0))])))]
        (assoc op :type :ok, :value res))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn non-monotonic-pairs-by-process
  "Given a history, and a function of an operation that extracts a numeric
  value, finds pairs of ops on the same process where that value decreases."
  [extractor history]
  (->> history
       (r/filter op/ok?)
       (reduce (fn [[last errs] op]
                 ; Last is a map of process ids to the last
                 ; operation we saw for that process. Errs is a
                 ; collection of error maps.
                 (let [p          (:process op)
                       value      (extractor op)
                       last-value (some-> (last p) extractor)]
                   (if (or (nil? last-value)
                           (<= (compare last-value value) 0))
                     ; Monotonic
                     [(assoc last p op) errs]
                     ; Non-monotonic!
                     [(assoc last p op)
                      (conj errs [(last p) op])])))
               [{} []])
       second))

(defn checker
  "This checks a single register's read and inc queries to ensure that clients
  observe a locally monotonic order. We generalize it using
  independent/checker."
  []
  (reify checker/Checker
    (check [_ test model history opts]
      (let [history (r/filter (comp #{:read :inc} :f) history)
            ; Check that register values are monotonic
            value-errs (->> history
                            (non-monotonic-pairs-by-process
                              (comp second :value)))
            ; Check that timestamps are monotonic
            ts-errs (->> history
                         (non-monotonic-pairs-by-process (comp first :value)))]
        {:valid? (and (empty? value-errs)
                      (empty? ts-errs))
         :value-errors value-errs
         :ts-errors    ts-errs}))))

(defn non-monotonic-pairs
  "Given a history, and a function of an operation that extracts a comparable
  value, finds pairs of ops where that value decreases."
  [extractor history]
  (info history)
  (->> history
       (partition 2 1)
       (keep (fn [[op1 op2 :as pair]]
                 (let [v1 (extractor op1)
                       v2 (extractor op2)]
                   (when-not (<= (compare v1 v2) 0)
                     pair))))))

(defn timestamp-value-checker
  "Checks a single register to ensure that the relationship between timestamps
  and register values is monotonic."
  []
  (reify checker/Checker
    (check [_ test model history opts]
      (let [errs (->> history
                      (r/filter op/ok?)
                      (r/filter (comp #{:read-at :inc} :f))
                      (into [])
                      (sort-by (comp first :value))
                      (non-monotonic-pairs-by-process (comp second :value)))]
        {:valid? (empty? errs)
         :errors errs}))))

(defn inc-gen
  [_ _]
  {:type :invoke, :f :inc, :value nil})

(defn read-gen
  [_ _]
  {:type :invoke, :f :read, :value nil})

(defn read-at-gen
  [_ _]
  {:type :invoke, :f :read-at :value [nil nil]})

(defn test
  [opts]
  (fauna/basic-test
    (merge {:client {:client (Client. nil)
                     :during (->> (gen/mix [inc-gen read-gen read-at-gen]))}
            :checker (checker/compose
                       {:perf (checker/perf)
                        :monotonic (checker)
                        :timestamp-value (timestamp-value-checker)})}
           opts)))
