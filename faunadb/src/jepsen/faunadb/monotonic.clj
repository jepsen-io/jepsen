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
                    [generator :as gen]
                    [independent :as independent]
                    [util :as util]
                    [store :as store]]
            [jepsen.checker.perf :as perf]
            [gnuplot.core :as g]
            [jepsen.faunadb [query :as q]
                            [client :as f]])
  (:import (java.time Instant)
           (java.time.temporal ChronoUnit)))

(def registers-name "registers")
(def registers (q/class registers-name))

(def k 0)

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

(defn restrict-history
  "FaunaDB appears to return infinite sequences of events for the history of
  single instances. This function takes a sequence of those events and cuts it
  off when it starts to loop over itself."
  [original-events]
  (loop [events original-events
         i      0
         seen   (transient #{})]
    (if-not (seq events)
       ; Finite sequence; done
       original-events

       (let [event (first events)]
         (if (seen event)
           ; This is a dup
           (take i original-events)
           (recur (next events) (inc i) (conj! seen event)))))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (f/with-retry
      (f/upsert-class! conn {:name registers-name})))

  (invoke! [this test op]
    (try
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
                                    v)
                                  ; Record doesn't exist, init to 1
                                  (q/do (q/create r {:data {:value 1}})
                                        0))])

                      :read    (f/query conn
                                        [(q/time "now")
                                         (q/if (q/exists? r)
                                           (q/select ["data" "value"] (q/get r))
                                           0)])

                      :read-at (let [ts (or (first v)
                                            (f/jitter-time
                                              (f/now conn)
                                              (:at-query-jitter test)))]
                                   (f/query conn
                                            [ts
                                             (q/at ts
                                                   (q/if (q/exists? r)
                                                     (q/select ["data" "value"]
                                                               (q/get r))
                                                     0))]))

                      ; This is broken--the next/prev pointers for history
                      ; traversal result in infinite loops.
                      :events (->> (f/query-all conn (q/events r))
                                   (take 1000)
                                   (map (juxt :ts
                                               :action
                                               (comp :value :data)))
                                   ))
              ; For increments and reads, convert timestamp to string
              v (case (:f op)
                  (:inc :read :read-at) (update (vec res) 0 strip-time)
                  :events               res)]
          (assoc op :type :ok, :value v)))
      (catch com.faunadb.client.errors.NotFoundException e
        (assoc op :type :fail, :error :not-found))))

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
    (check [_ test history opts]
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
  (->> history
       (partition 2 1)
       (keep (fn [[op1 op2 :as pair]]
                 (let [v1 (extractor op1)
                       v2 (extractor op2)]
                   (when-not (<= (compare v1 v2) 0)
                     pair))))))

(defn timestamp-value-checker
  "Checks a single register to ensure that the relationship between timestamps
  and register values is globally monotonic."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [errs (->> history
                      (r/filter op/ok?)
                      (r/filter (comp #{:read-at :inc} :f))
                      (into [])
                      (sort-by (comp first :value))
                      (non-monotonic-pairs (comp second :value)))]
        {:valid? (empty? errs)
         :errors errs}))))

(defn merged-windows
  "Takes a collection of points, and computes [lower, upper] windows of s
  elements before and after each point, then merges overlapping windows
  together.

  s determines the size of the window, in... points, I think?"
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

(defn timestamp-value-plot!
  "Renders a plot of the value of a register over different timestamps. Takes a
  test and checker opts to determine the subdirectory to write to."
  [test opts filename history]
  (let [t0     (-> test :start-time .getMillis Instant/ofEpochMilli)
        series (->> history
                    (r/filter (comp number? :process))
                    (r/filter #(= :ok (:type %)))
                    (group-by :process)
                    (util/map-vals
                      (partial mapv (fn [op]
                                      [; Convert fauna timestamp to seconds
                                       ; since start of test
                                       (let [t (-> (:value op)
                                                   first
                                                   (stripped-time->instant))
                                             dt (-> t0
                                                    (.until t ChronoUnit/NANOS)
                                                    (util/nanos->secs))]
                                         dt)
                                       ; Observed value
                                       (second (:value op))]))))
        colors (perf/qs->colors (keys series))
        path (.getCanonicalPath
               (store/path! test (:subdirectory opts)
                            (str "sequential " filename ".png")))]
    (try
      (let [plot {:preamble (concat (perf/preamble path)
                               [[:set :title (str (:name test) " sequential by process")]
                                [:set :ylabel "register value"]
                                [:set :xlabel "faunadb timestamp"]])
                  :series (map (fn [process points]
                                 {:title (str process)
                                  :with :linespoints
                                  :data points
                                  :pointtype 2
                                  :linetype (colors process)})
                               series)}]
        (when (perf/has-data? plot)
          (-> plot
              (perf/without-empty-series)
              (perf/with-range)
              (perf/with-nemeses history (:nemeses (:plot test)))
              (perf/plot!))))
      {:valid? true}
      (catch java.io.IOException _
        (throw (IllegalStateException. "Error rendering plot; verify gnuplot is installed and reachable"))))))

(defn timestamp-value-plotter
  "Plots interesting bits of the value as seen by each process history."
  []
  (reify checker/Checker
    (check [this test history opts]
      ; Identify interesting regions
      (let [; Set aside nemesis operations so we can plot them later
            nemesis-history (r/filter (comp #{:nemesis} :process) history)
            ; Extract temporal reads and sort by timestamp
            history (->> history
                         (r/filter op/ok?)
                         (r/filter (comp #{:read-at} :f))
                         (into [])
                         (sort-by (comp first :value))
                         vec)
            extractor (comp second :value)
            spots (nth (reduce (fn [[i last spots] op]
                                 ; Figure out if this is a spot
                                 (let [p  (:process op)
                                       v  (some-> (last p) extractor)
                                       v' (extractor op)]
                                   [(inc i)
                                    (assoc last p op)
                                    (if (or (nil? v) (<= 0 (compare v v')))
                                      ; Monotonic
                                      spots
                                      ; Non-monotonic
                                      (conj spots i))]))
                               [0 {} []]
                               history)
                       2)]
        (->> spots
             (merged-windows 32)
             (map-indexed (fn [i [lower upper]]
                            (->> (subvec history
                                         (max lower 0)
                                         (min upper (dec (count history))))
                                 (timestamp-value-plot! test opts i))))
             dorun))
      {:valid? true})))

(defn not-found-checker
  "We do explicit existence checks before all reads, and should never observe a
  not-found result. Let's make sure of that."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [errs (->> history
                      (r/filter op/fail?)
                      (r/filter (comp #{:not-found} :error))
                      (into []))]
        {:valid? (empty? errs)
         :invoke-count (->> history (r/filter op/invoke?) (into []) count)
         :error-count (count errs)
         :first  (first errs)
         :last   (peek errs)}))))

(defn inc-gen
  [_ _]
  {:type :invoke, :f :inc, :value nil})

(defn read-gen
  [_ _]
  {:type :invoke, :f :read, :value nil})

(defn read-at-gen
  [_ _]
  {:type :invoke, :f :read-at :value [nil nil]})

(defn workload
  [opts]
  (let [n (count (:nodes opts))]
    {:client    (Client. nil)
     :generator (gen/mix [inc-gen read-gen read-at-gen])
     :final-generator (gen/once {:type :invoke, :f :events})
     :checker (checker/compose
                {:monotonic (checker)
                 :not-found (not-found-checker)
                 :timestamp-value-plot (timestamp-value-plotter)
                 :timestamp-value (timestamp-value-checker)})}))
