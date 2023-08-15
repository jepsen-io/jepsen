(ns jepsen.util
  "Kitchen sink"
  (:refer-clojure :exclude [parse-long]) ; Clojure added this in 1.11.1
  (:require [clojure.tools.logging :refer [info]]
            [clojure.core.reducers :as r]
            [clojure [string :as str]
                     [pprint :as pprint :refer [pprint]]
                     [walk :as walk]]
            [clojure.java [io :as io]
                          [shell :as shell]]
            [clj-time.core :as time]
            [clj-time.local :as time.local]
            [clojure.tools.logging :refer [debug info warn]]
            [dom-top.core :as dt :refer [bounded-future]]
            [fipp [edn :as fipp]
                  [ednize]
                  [engine :as fipp.engine]]
            [jepsen [history :as h]]
            [jepsen.history.fold :refer [loopf]]
            [potemkin :refer [definterface+]]
            [slingshot.slingshot :refer [try+ throw+]]
            [tesser.core :as t])
  (:import (java.lang.reflect Method)
           (java.util.concurrent.locks LockSupport)
           (java.util.concurrent ExecutionException)
           (java.io File
                    RandomAccessFile)
           (jepsen.history Op)))


(defn default
  "Like assoc, but only fills in values which are NOT present in the map."
  [m k v]
  (if (contains? m k)
    m
    (assoc m k v)))

(defn exception?
  "Is x an Exception?"
  [x]
  (instance? Exception x))

(defn fcatch
  "Takes a function and returns a version of it which returns, rather than
  throws, exceptions."
  [f]
  (fn wrapper [& args]
    (try (apply f args)
         (catch Exception e e))))

(defn random-nonempty-subset
  "A randomly selected, randomly ordered, non-empty subset of the given
  collection. Returns nil if collection is empty."
  [coll]
  (when (seq coll)
    (take (inc (rand-int (count coll))) (shuffle coll))))

(defn name+
  "Tries name, falls back to pr-str."
  [x]
  (if (instance? clojure.lang.Named x)
    (name x)
    (pr-str x)))

(def uninteresting-exceptions
  "Exceptions which are less interesting; used by real-pmap and other cases where we want to pick a *meaningful* exception."
  #{java.util.concurrent.BrokenBarrierException
    java.util.concurrent.TimeoutException
    InterruptedException})

(defn real-pmap
  "Like pmap, but runs a thread per element, which prevents deadlocks when work
  elements have dependencies. The dom-top real-pmap throws the first exception
  it gets, which might be something unhelpful like InterruptedException or
  BrokenBarrierException. This variant works like that real-pmap, but throws
  more interesting exceptions when possible."
  [f coll]
  (let [[results exceptions] (dt/real-pmap-helper f coll)]
    (when (seq exceptions)
      (throw (or (first (remove (comp uninteresting-exceptions class)
                                exceptions))
                 (first exceptions))))
    results))

(defn processors
  "How many processors on this platform?"
  []
  (.. Runtime getRuntime availableProcessors))

(defn majority
  "Given a number, returns the smallest integer strictly greater than half."
  [n]
  (inc (int (Math/floor (/ n 2)))))

(defn minority-third
  "Given a number, returns the largest integer strictly less than 1/3rd.
  Helpful for testing byzantine fault-tolerant systems."
  [n]
  (-> n dec (/ 3) long))

(defn min-by
  "Finds the minimum element of a collection based on some (f element), which
  returns Comparables. If `coll` is empty, returns nil."
  [f coll]
  (when (seq coll)
    (reduce (fn [m e]
              (if (pos? (compare (f m) (f e)))
                e
                m))
            coll)))

(defn max-by
  "Finds the maximum element of a collection based on some (f element), which
  returns Comparables. If `coll` is empty, returns nil."
  [f coll]
  (when (seq coll)
    (reduce (fn [m e]
              (if (neg? (compare (f m) (f e)))
                e
                m))
            coll)))

(defn fast-last
  "Like last, but O(1) on counted collections."
  [coll]
  (nth coll (dec (count coll))))

(defn rand-nth-empty
  "Like rand-nth, but returns nil if the collection is empty."
  [coll]
  (try (rand-nth coll)
       (catch IndexOutOfBoundsException e nil)))

(defn rand-exp
  "Generates a exponentially distributed random value with rate parameter
  lambda."
  [lambda]
  (* (Math/log (- 1 (rand))) (- lambda)))

(defn rand-distribution
  "Generates a random value with a distribution (default `:uniform`) of:
   ```clj
   ; Uniform distribution from min (inclusive, default 0) to max (exclusive, default Long/MAX_VALUE). 
   {:distribution :uniform, :min 0, :max 1024}

   ; Geometric distribution with mean 1/p.
   {:distribution :geometric, :p 1e-3}

   ; Select a value from a sequence with equal probability.
   {:distribution :one-of, :values [-1, 4097, 1e+6]}

   ; Select a value based on weights. :weights are {value weight ...}
   {:distribution :weighted :weights {1e-3 1 1e-4 3 1e-5 1}}
   ```"
  ([] (rand-distribution {}))
  ([distribution-map]
   (let [{:keys [distribution min max p values weights]} distribution-map
         distribution (or distribution :uniform)
         min (or min 0)
         max (or max Long/MAX_VALUE)
         _   (assert (case distribution
                       :uniform   (< min max)
                       :geometric (number? p)
                       :one-of    (seq values)
                       :weighted  (and (map? weights)
                                       (->> weights
                                            vals
                                            (every? number?)))
                       false)
                     (str "Invalid distribution-map: " distribution-map))]
     (case distribution
       :uniform   (long (Math/floor (+ min (* (rand) (- max min)))))
       :geometric (long (Math/ceil  (/ (Math/log (rand))
                                       (Math/log (- 1.0 p)))))
       :one-of    (rand-nth values)
       :weighted  (let [values  (keys weights)
                        weights (reductions + (vals weights))
                        total   (last weights)
                        choices (map vector values weights)]
                    (let [choice (rand-int total)]
                      (loop [[[v w] & more] choices]
                        (if (< choice w)
                          v
                          (recur more)))))))))

(defn fraction
  "a/b, but if b is zero, returns unity."
  [a b]
  (if (zero? b)
    1
    (/ a b)))

(defn inc*
  "Like inc, but (inc nil) => 1."
  [x]
  (if (nil? x)
    1
    (inc x)))

(defn local-time
  "Local time."
  []
  (time.local/local-now))

(defn chunk-vec
  "Partitions a vector into reducibles of size n (somewhat like partition-all)
  but uses subvec for speed.

      (chunk-vec 2 [1])     ; => ([1])
      (chunk-vec 2 [1 2 3]) ; => ([1 2] [3])"
   ([^long n v]
   (let [c (count v)]
     (->> (range 0 c n)
          (map #(subvec v % (min c (+ % n))))))))

(def buf-size 1048576)

(defn concat-files!
  "Appends contents of all fs, writing to out. Returns fs."
  [out fs]
  (with-open [oc (.getChannel (RandomAccessFile. (io/file out) "rw"))]
    (doseq [f fs]
      (with-open [fc (.getChannel (RandomAccessFile. (io/file f) "r"))]
        (let [size (.size fc)]
          (loop [position 0]
            (when (< position size)
              (recur (+ position (.transferTo fc
                                              position
                                              (min (- size position)
                                                   buf-size)
                                              oc)))))))))
  fs)

(defn op->str
  "Format an operation as a string."
  [op]
  (str (:process op)         \tab
       (:type op)            \tab
       (pr-str (:f op))      \tab
       (pr-str (:value op))
       (when-let [err (:error op)]
         (str \tab err))))

(defn prn-op
  "Prints an operation to the console."
  [op]
  (pr (:process op)) (print \tab)
  (pr (:type op))    (print \tab)
  (pr (:f op))       (print \tab)
  (pr (:value op))
  (when-let [err (:error op)]
    (print \tab) (print err))
  (print \newline))

(defn print-history
  "Prints a history to the console."
  ([history]
    (print-history prn-op history))
  ([printer history]
   (doseq [op history]
     (printer op))))

(defn write-history!
  "Writes a history to a file."
  ([f history]
   (write-history! f prn-op history))
  ([f printer history]
   (with-open [w (io/writer f)]
     (binding [*out* w]
       (print-history printer history)))))

(defn pwrite-history!
  "Writes history, taking advantage of more cores."
  ([f history]
    (pwrite-history! f prn-op history))
  ([f printer history]
   (h/fold history
           (loopf {:name [:pwrite-history (str printer)]}
                  ; Reduce
                  ([file   (File/createTempFile "jepsen-history" ".part")
                    writer (io/writer file)]
                   [op]
                   (do (binding [*out*              writer
                                 *flush-on-newline* false]
                         (printer op))
                       (recur file writer))
                   (do (.flush ^java.io.Writer writer)
                       (.close ^java.io.Writer writer)
                       file))
                  ; Combine
                  ([files []]
                   [file]
                   (recur (conj files file))
                   (try (concat-files! f files)
                        f
                        (finally
                          (doseq [^File f files] (.delete f)))))))))

(defn log-op
  "Logs an operation and returns it."
  [op]
  (info (op->str op))
  op)

(def logger (agent nil))

(defn log-print
      [_ & things]
      (apply println things))

(defn log
      [& things]
      (apply send-off logger log-print things))

(defn test->str
  "Pretty-prints a test to a string. This binds *print-length* to avoid printing
  infinite sequences for generators."
  [test]
  ; What we're doing here is basically recreating normal map pretty-printing at
  ; the top level, but overriding generators so that they only print 8 or so
  ; elements.
  (with-out-str
    (fipp.engine/pprint-document
      [:group "{"
       [:nest 1
        (->> test
             (map (fn [[k v]]
                    [:group
                     (fipp/pretty k)
                     :line
                     (if (= k :generator)
                       (binding [*print-length* 8]
                         (fipp/pretty v))
                       (fipp/pretty v))]))
             (interpose [:line ", " ""]))]
        "}"]
      {:width 80})))

;(defn all-loggers []
;  (->> (org.apache.log4j.LogManager/getCurrentLoggers)
;       (java.util.Collections/list)
;       (cons (org.apache.log4j.LogManager/getRootLogger))))

(defn all-jdk-loggers []
  (let [manager (java.util.logging.LogManager/getLogManager)]
    (->> manager
         .getLoggerNames
         java.util.Collections/list
         (map #(.getLogger manager %)))))

(defmacro mute-jdk [& body]
  `(let [loggers# (all-jdk-loggers)
         levels#  (map #(.getLevel %) loggers#)]
     (try
       (doseq [l# loggers#]
         (.setLevel l# java.util.logging.Level/OFF))
       ~@body
       (finally
         (dorun (map (fn [logger# level#] (.setLevel logger# level#))
                     loggers#
                     levels#))))))

;(defmacro mute-log4j [& body]
;  `(let [loggers# (all-loggers)
;         levels#  (map #(.getLevel %) loggers#)]
;     (try
;       (doseq [l# loggers#]
;         (.setLevel l# org.apache.log4j.Level/OFF))
;       ~@body
;       (finally
;         (dorun (map (fn [logger# level#] (.setLevel logger# level#))
;                     loggers#
;                     levels#))))))

(defmacro mute [& body]
  `(mute-jdk
;     (mute-log4j
       ~@body));)

(defn ms->nanos [ms] (* ms 1000000))

(defn nanos->ms [nanos] (/ nanos 1000000))

(defn secs->nanos [s] (* s 1e9))

(defn nanos->secs [nanos] (/ nanos 1e9))

(defn ^Long linear-time-nanos
  "A linear time source in nanoseconds."
  []
  (System/nanoTime))

(def ^:dynamic ^Long *relative-time-origin*
  "A reference point for measuring time in a test run."
  nil)

(defmacro with-relative-time
  "Binds *relative-time-origin* at the start of body."
  [& body]
  `(binding [*relative-time-origin* (linear-time-nanos)]
     (info "Relative time begins now")
     ~@body))

(defn relative-time-nanos
  "Time in nanoseconds since *relative-time-origin*"
  []
  (- (linear-time-nanos) *relative-time-origin*))

(defn sleep
  "High-resolution sleep; takes a (possibly fractional) time in ms."
  [dt]
  (let [t (+ (long (ms->nanos dt))
                    (System/nanoTime))]
    (while (< (+ (System/nanoTime) 10000) t)
      (LockSupport/parkNanos (- t (System/nanoTime))))))

(defmacro time-
  [& body]
  `(let [t0# (System/nanoTime)]
    ~@body
     (nanos->ms (- (System/nanoTime) t0#))))

(defn pprint-str [x]
  (with-out-str (fipp/pprint x {:width 78})))

(defn spy [x]
  (info (pprint-str x))
  x)

(defmacro timeout
  "Times out body after n millis, returning timeout-val."
  [millis timeout-val & body]
  `(let [worker# (future ~@body)
         retval# (try
                   (deref worker# ~millis ::timeout)
                   (catch ExecutionException ee#
                     (throw (.getCause ee#))))]
     (if (= retval# ::timeout)
       (do (future-cancel worker#)
           ~timeout-val)
       retval#)))

(defn await-fn
  "Invokes a function (f) repeatedly. Blocks until (f) returns, rather than
  throwing. Returns that return value. Catches Exceptions (except for
  InterruptedException) and retries them automatically. Options:

    :retry-interval   How long between retries, in ms. Default 1s.
    :log-interval     How long between logging that we're still waiting, in ms.
                      Default `retry-interval.
    :log-message      What should we log to the console while waiting?
    :timeout          How long until giving up and throwing :type :timeout, in
                      ms. Default 60 seconds."
  ([f]
   (await-fn f {}))
  ([f opts]
   (let [log-message    (:log-message opts (str "Waiting for " f "..."))
         retry-interval (:retry-interval opts 1000)
         log-interval   (:log-interval opts retry-interval)
         timeout        (:timeout opts 60000)
         t0             (linear-time-nanos)
         log-deadline   (atom (+ t0 (* 1e6 log-interval)))
         deadline       (+ t0 (* 1e6 timeout))]
     (loop []
       (let [res (try
                   (f)
                   (catch InterruptedException e
                     (throw e))
                   (catch Exception e
                     (let [now (linear-time-nanos)]
                       ; Are we out of time?
                       (when (<= deadline now)
                         (throw+ {:type :timeout} e))

                       ; Should we log something?
                       (when (<= @log-deadline now)
                         (info log-message)
                         (swap! log-deadline + (* log-interval 1e6)))

                       ; Right, sleep and retry
                       (Thread/sleep retry-interval)
                       ::retry)))]
         (if (= ::retry res)
           (recur)
           res))))))

(defmacro retry
  "Evals body repeatedly until it doesn't throw, sleeping dt seconds."
  [dt & body]
  `(loop []
     (let [res# (try ~@body
                     (catch Throwable e#
;                      (warn e# "retrying in" ~dt "seconds")
                       ::failed))]
       (if (= res# ::failed)
         (do (Thread/sleep (* ~dt 1000))
             (recur))
         res#))))

(defrecord Retry [bindings])

(defmacro with-retry
  "It's really fucking inconvenient not being able to recur from within (catch)
  expressions. This macro wraps its body in a (loop [bindings] (try ...)).
  Provides a (retry & new bindings) form which is usable within (catch) blocks:
  when this form is returned by the body, the body will be retried with the new
  bindings."
  [initial-bindings & body]
  (assert (vector? initial-bindings))
  (assert (even? (count initial-bindings)))
  (let [bindings-count (/ (count initial-bindings) 2)
        body (walk/prewalk (fn [form]
                             (if (and (seq? form)
                                      (= 'retry (first form)))
                               (do (assert (= bindings-count
                                              (count (rest form))))
                                   `(Retry. [~@(rest form)]))
                               form))
                           body)
        retval (gensym 'retval)]
    `(loop [~@initial-bindings]
       (let [~retval (try ~@body)]
        (if (instance? Retry ~retval)
          (recur ~@(->> (range bindings-count)
                        (map (fn [i] `(nth (.bindings ~retval) ~i)))))
          ~retval)))))

(deftype Return [value])

(defn letr-rewrite-return
  "Rewrites (return x) to (Return. x) in expr. Returns a pair of [changed?
  expr], where changed is whether the expression contained a return."
  [expr]
  (let [return? (atom false)
        expr    (walk/prewalk
                  (fn [form]
                    (if (and (seq? form)
                             (= 'return (first form)))
                      (do (assert
                            (= 2 (count form))
                            (str (pr-str form) " should have one argument"))
                          (reset! return? true)
                          `(Return. ~(second form)))
                      form))
                  expr)]
    [@return? expr]))

(defn letr-partition-bindings
  "Takes a vector of bindings [sym expr, sym' expr, ...]. Returns
  binding-groups: a sequence of vectors of bindgs, where the final binding in
  each group has an early return. The final group (possibly empty!) contains no
  early return."
  [bindings]
  (->> bindings
       (partition 2)
       (reduce (fn [groups [sym expr]]
                 (let [[return? expr] (letr-rewrite-return expr)
                       groups (assoc groups
                                     (dec (count groups))
                                     (-> (peek groups) (conj sym) (conj expr)))]
                   (if return?
                     (do (assert (symbol? sym)
                                 (str (pr-str sym " must be a symbol")))
                         (conj groups []))
                     groups)))
               [[]])))

(defn letr-let-if
  "Takes a sequence of binding groups and a body expression, and emits a let
  for the first group, an if statement checking for a return, and recurses;
  ending with body."
  [groups body]
  (assert (pos? (count groups)))
  (if (= 1 (count groups))
    ; Final group with no returns
    `(let ~(first groups) ~@body)

    ; Group ending in a return
    (let [bindings  (first groups)
          final-sym (nth bindings (- (count bindings) 2))]
      `(let ~bindings
         (if (instance? Return ~final-sym)
           (.value ~final-sym)
           ~(letr-let-if (rest groups) body))))))

(defmacro letr
  "Let bindings, plus early return.

  You want to do some complicated, multi-stage operation assigning lots of
  variables--but at different points in the let binding, you need to perform
  some conditional check to make sure you can proceed to the next step.
  Ordinarily, you'd intersperse let and if statements, like so:

      (let [res (network-call)]
        (if-not (:ok? res)
          :failed-network-call

          (let [people (:people (:body res))]
            (if (zero? (count people))
              :no-people

              (let [res2 (network-call-2 people)]
                ...

  This is a linear chain of operations, but we're forced to nest deeply because
  we have no early-return construct. In ruby, we might write

      res = network_call
      return :failed_network_call if not x.ok?

      people = res[:body][:people]
      return :no-people if people.empty?

      res2 = network_call_2 people
      ...

  which reads the same, but requires no nesting thanks to Ruby's early return.
  Clojure's single-return is *usually* a boon to understandability, but deep
  linear branching usually means something like

    - Deep nesting         (readability issues)
    - Function chaining    (lots of arguments for bound variables)
    - Throw/catch          (awkward exception wrappers)
    - Monadic interpreter  (slow, indirect)

  This macro lets you write:

      (letr [res    (network-call)
             _      (when-not (:ok? res) (return :failed-network-call))
             people (:people (:body res))
             _      (when (zero? (count people)) (return :no-people))
             res2   (network-call-2 people)]
        ...)

  letr works like let, but if (return x) is ever returned from a binding, letr
  returns x, and does not evaluate subsequent expressions.

  If something other than (return x) is returned from evaluating a binding,
  letr binds the corresponding variable as normal. Here, we use _ to indicate
  that we're not using the results of (when ...), but this is not mandatory.
  You cannot use a destructuring bind for a return expression.

  letr is not a *true* early return--(return x) must be a *terminal* expression
  for it to work--like (recur). For example,

      (letr [x (do (return 2) 1)]
        x)

  returns 1, not 2, because (return 2) was not the terminal expression.

  return only works within letr's bindings, not its body."
  [bindings & body]
  (assert (vector? bindings))
  (assert (even? (count bindings)))
  (let [groups (letr-partition-bindings bindings)]
    (letr-let-if (letr-partition-bindings bindings) body)))

(defn map-kv
  "Takes a function (f [k v]) which returns [k v], and builds a new map by
  applying f to every pair."
  [f m]
  (into {} (r/map f m)))

(defn map-keys
  "Maps keys in a map."
  [f m]
  (map-kv (fn [[k v]] [(f k) v]) m))

(defn map-vals
  "Maps values in a map."
  [f m]
  (map-kv (fn [[k v]] [k (f v)]) m))

(defn compare<
  "Like <, but works on any comparable objects, not just numbers."
  [a b]
  (neg? (compare a b)))

(defn poly-compare
  "Comparator function for sorting heterogenous collections."
  [a b]
  (try (compare a b)
       (catch java.lang.ClassCastException e
         (compare (str (class a)) (str (class b))))))

(defn polysort
  "Sort, but on heterogenous collections."
  [coll]
  (sort poly-compare coll))

(defn integer-interval-set-str
  "Takes a set of integers and yields a sorted, compact string representation."
  [set]
  (if (some nil? set)
    (str set)
    (let [[runs start end]
          (reduce (fn r [[runs start end] cur]
                    (cond ; Start new run
                          (nil? start) [runs cur cur]

                          ; Continue run
                          (= cur (inc end)) [runs start cur]

                          ; Break!
                          :else [(conj runs [start end]) cur cur]))
                  [[] nil nil]
                  (sort set))
          runs (if (nil? start) runs (conj runs [start end]))]
      (str "#{"
           (->> runs
                (map (fn m [[start end]]
                       (if (= start end)
                         start
                         (str start ".." end))))
                (str/join " "))
           "}"))))

(defmacro meh
  "Returns, rather than throws, exceptions."
  [& body]
  `(try ~@body (catch Exception e# e#)))

(defmacro with-thread-name
  "Sets the thread name for duration of block."
  [thread-name & body]
  `(let [old-name# (.. Thread currentThread getName)]
     (try
       (.. Thread currentThread (setName (name ~thread-name)))
       ~@body
       (finally (.. Thread currentThread (setName old-name#))))))

(defn maybe-number
  "Tries reading a string as a long, then double, then string. Passes through
  nil. Useful for getting nice values out of stats APIs that just dump a bunch
  of heterogenously-typed strings at you."
  [s]
  (when s
    (try (Long/parseLong s)
         (catch java.lang.NumberFormatException e
           (try (Double/parseDouble s)
                (catch java.lang.NumberFormatException e
                  s))))))

(defn coll
  "Wraps non-collection things into singleton lists, and leaves colls as
  themselves. Useful when you can take either a single thing or a sequence of
  things."
  [thing-or-things]
  (cond (nil? thing-or-things)  nil
        (coll? thing-or-things) thing-or-things
        true                    (list thing-or-things)))

(defn sequential
  "Wraps non-sequential things into singleton lists, and leaves sequential
  things or nil as themselves. Useful when you can take either a single thing
  or a sequence of things."
  [thing-or-things]
  (cond (nil? thing-or-things)        nil
        (sequential? thing-or-things) thing-or-things
        true                          (list thing-or-things)))

(defn history->latencies
  "Takes a history--a sequence of operations--and returns a new history where
  operations have two new keys:

  :latency    the time in nanoseconds it took for the operation to complete.
  :completion the next event for that process"
  [history]
  (h/ensure-pair-index history)
  (h/map (fn add-latency [^Op op]
           (if (h/invoke? op)
             (if-let [^Op c (h/completion history op)]
               (assoc op
                      :completion c
                      :latency (- (.time c) (.time op)))
               op)
             op))
         history))

(defn nemesis-intervals
  "Given a history where a nemesis goes through :f :start and :f :stop type
  transitions, constructs a sequence of pairs of start and stop ops. Since a
  nemesis usually goes :start :start :stop :stop, we construct pairs of the
  first and third, then second and fourth events. Where no :stop op is present,
  we emit a pair like [start nil]. Optionally, a map of start and stop sets may
  be provided to match on user-defined :start and :stop keys.

  Multiple starts are ended by the same pair of stops, so :start1 :start2
  :start3 :start4 :stop1 :stop2 yields:

    [start1 stop1]
    [start2 stop2]
    [start3 stop1]
    [start4 stop2]"
  ([history]
   (nemesis-intervals history {}))
  ([history opts]
   ;; Default to :start and :stop if no region keys are provided
   (let [start (:start opts #{:start})
         stop  (:stop  opts #{:stop})
         [intervals starts]
         ; First, group nemesis ops into pairs (one for invoke, one for
         ; complete)
         (->> history
              (filter #(= :nemesis (:process %)))
              (partition 2)
              ; Verify that every pair has identical :fs. It's possible
              ; that nemeses might, some day, log more types of :info
              ; ops, maybe not in a call-response pattern, but that'll
              ; break us.
              (filter (fn [[a b]] (= (:f a) (:f b))))
              ; Now move through all nemesis ops, keeping track of all start
              ; pairs, and closing those off when we see a stop pair.
              (reduce (fn [[intervals starts :as state] [a b :as pair]]
                        (let [f (:f a)]
                          (cond (start f)  [intervals (conj starts pair)]
                                (stop f)   [(->> starts
                                                 (mapcat (fn [[s1 s2]]
                                                           [[s1 a] [s2 b]]))
                                                 (into intervals))
                                            []]
                                true        state)))
                      [[] []]))]
     ; Complete unfinished intervals
     (into intervals (mapcat (fn [[s1 s2]] [[s1 nil] [s2 nil]]) starts)))))


(defn longest-common-prefix
  "Given a collection of sequences, finds the longest sequence which is a
  prefix of every sequence given."
  [cs]
  (when (seq cs)
    (reduce (fn prefix [s1 s2]
              (let [len (->> (map = s1 s2)
                             (take-while true?)
                             count)]
                ; Avoid unnecessary seq wrapping
                (if (= len (count s1))
                  s1
                  (take len s2))))
            cs)))

(defn drop-common-proper-prefix
  "Given a collection of sequences, removes the longest common proper prefix
  from each one."
  [cs]
  (map (partial drop (reduce min
                             (count (longest-common-prefix cs))
                             (map (comp dec count) cs)))
       cs))

(definterface ILazyAtom
  (init []))

(defn lazy-atom
  "An atom with lazy state initialization. Calls (f) on first use to provide
  the initial value of the atom. Only supports swap/reset/deref. Reset bypasses
  lazy initialization. If f throws, behavior is undefined (read: proper
  fucked)."
  [f]
  (let [state ^clojure.lang.Atom (atom ::fresh)]
    (reify
      ILazyAtom
      (init [_]
        (let [s @state]
          (if-not (identical? s ::fresh)
            ; Regular old value
            s

            ; Someone must initialize. Everyone form an orderly queue.
            (do (locking state
                  (if (identical? @state ::fresh)
                    ; We're the first.
                    (reset! state (f))))

                ; OK, definitely initialized now.
                @state))))

      clojure.lang.IAtom
      (swap [this f]
        (.init this)
        (.swap state f))
      (swap [this f a]
        (.init this)
        (.swap state f a))
      (swap [this f a b]
        (.init this)
        (.swap state f a b))
      (swap [this f a b more]
        (.init this)
        (.swap state f a b more))

      (compareAndSet [this v v']
        (.init this)
        (.compareAndSet state v v'))

      (reset [this v]
        (.reset state v))

      clojure.lang.IDeref
      (deref [this]
        (.init this)))))

(defn named-locks
  "Creates a mutable data structure which backs a named locking mechanism.

  Named locks are helpful when you need to coordinate access to a dynamic pool
  of resources. For instance, you might want to prohibit multiple threads from
  executing a command on a remote node at once. Nodes are uniquely identified
  by a string name, so you could write:

      (defonce node-locks (named-locks))

      ...
      (defn start-db! [node]
        (with-named-lock node-locks node
          (c/exec :service :meowdb :start)))

  Now, concurrent calls to start-db! will not execute concurrently.

  The structure we use to track named locks is an atom wrapping a map, where
  the map's keys are any object, and the values are canonicalized versions of
  that same object. We use standard Java locking on the canonicalized versions.
  This is basically an arbitrary version of string interning."
  []
  (atom {}))

(defn get-named-lock!
  "Given a pool of locks, and a lock name, returns the object used for locking
  in that pool. Creates the lock if it does not already exist."
  [locks name]
  (-> locks
      (swap! (fn [locks]
               (if-let [o (get locks name)]
                 locks
                 (assoc locks name name))))
      (get name)))

(defmacro with-named-lock
  "Given a lock pool, and a name, locks that name in the pool for the duration
  of the body."
  [locks name & body]
  `(locking (get-named-lock! ~locks ~name) ~@body))

(defn contains-many?
  "Takes a map and any number of keys, returning true if all of the keys are
  present. Ex. (contains-many? {:a 1 :b 2 :c 3} :a :b :c) => true"
  [m & ks]
  (every? #(contains? m %) ks))

(defn parse-long
  "Parses a string to a Long. Look, we use this a lot, okay?"
  [s]
  (Long/parseLong s))

(defn ex-root-cause
  "Unwraps throwables to return their original cause."
  [^Throwable t]
  (if-let [cause (.getCause t)]
    (recur cause)
    t))

(defn arities
  "The arities of a function class."
  [^Class c]
  (keep (fn [^Method method]
          (when (re-find #"invoke" (.getName method))
            (alength (.getParameterTypes method))))
        (-> c .getDeclaredMethods)))

(defn fixed-point
  "Applies f repeatedly to x until it converges."
  [f x]
  (let [x' (f x)]
    (if (= x x')
      x
      (recur f x'))))

(defn sh
  "A wrapper around clojure.java.shell's sh which throws on nonzero exit."
  [& args]
  (let [res (apply shell/sh args)]
    (when-not (zero? (:exit res))
      (throw+ (assoc res :type ::nonzero-exit)
              (str "Shell command " (pr-str args)
                   " returned exit status " (:exit res) "\n"
                   (:out res) "\n"
                   (:err res))))
    res))

(defn deepfind
  "Finds things that match a predicate in a nested structure. Returns a
  lazy sequence of matching things, each represented by a vector *path* which
  denotes how to access that object, ending in the matching thing itself. Path
  elements are:

    - keys for maps
    - integers for sequentials
    - :member for sets
    - :deref  for deref-ables.

    (deepfind string? [:a {:b \"foo\"} :c])
    ; => ([1 :b \"foo\"])
  "
  ([pred haystack]
   (deepfind pred [] haystack))
  ([pred path haystack]
   (cond ; This is a match; we're done
         (pred haystack)
         [(conj path haystack)]

         (map? haystack)
         (mapcat (fn [[k v]]
                   (deepfind pred (conj path k) v))
                 haystack)

         (sequential? haystack)
         (->> haystack
              (map-indexed (fn [i x] (deepfind pred (conj path i) x)))
              (mapcat identity))

         (set? haystack)
         (mapcat (partial deepfind pred (conj path :member)) haystack)

         (instance? clojure.lang.IDeref haystack)
         (deepfind pred (conj path :deref) @haystack)

         true
         nil)))

(definterface+ IForgettable
  (forget! [this]
           "Allows this forgettable reference to be reclaimed by the GC at some
           later time. Future attempts to dereference it may throw. Returns
           self."))

(deftype Forgettable [^:unsynchronized-mutable x]
  IForgettable
  (forget! [this]
    (set! x ::forgotten)
    this)

  clojure.lang.IDeref
  (deref [this]
    (let [x x]
      (if (identical? x ::forgotten)
        (throw+ {:type ::forgotten})
        x)))

  Object
  (toString [this]
    (let [x x]
      (str "#<Forgettable " (if (identical? x ::forgotten)
                              "?"
                              x)
           ">")))

  (equals [this other]
    (identical? this other)))

(defn forgettable
  "Constructs a deref-able reference to x which can be explicitly forgotten.
  Helpful for controlling access to infinite seqs (e.g. the generator) when you
  don't have firm control over everyone who might see them."
  [x]
  (Forgettable. x))

(defmethod pprint/simple-dispatch jepsen.util.Forgettable
  [^Forgettable f]
  (let [prefix (format "#<Forgettable ")]
    (pprint/pprint-logical-block
      :prefix prefix :suffix ">"
      (pprint/pprint-indent :block (-> (count prefix) (- 2) -))
      (pprint/pprint-newline :linear)
      (pprint/write-out (try+ @f
                              (catch [:type ::forgotten] e
                                "?"))))))

(prefer-method pprint/simple-dispatch
               jepsen.util.Forgettable clojure.lang.IDeref)

(extend-protocol fipp.ednize/IOverride jepsen.util.Forgettable)
(extend-protocol fipp.ednize/IEdn jepsen.util.Forgettable
  (-edn [f]
    (fipp.ednize/tagged-object f
                               (try+ @f
                                     (catch [:type ::forgotten] e
                                       '?)))))

