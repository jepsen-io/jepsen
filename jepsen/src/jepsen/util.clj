(ns jepsen.util
  "Kitchen sink"
  (:require [clojure.tools.logging :refer [info]]
            [clojure.core.reducers :as r]
            [clojure.string :as str]
            [clj-time.core :as time]
            [clj-time.local :as time.local])
  (:import (java.util.concurrent.locks LockSupport)))

(defn majority
  "Given a number, returns the smallest integer strictly greater than half."
  [n]
  (inc (int (Math/floor (/ n 2)))))

(defn fraction
  "a/b, but if b is zero, returns unity."
  [a b]
  (if (zero? b)
    1
    (/ a b)))

(defn local-time
  "Drops millisecond resolution"
  []
  (let [t (time.local/local-now)]
    (time/minus t (time/millis (time/milli t)))))

(defn op->str
  "Format an operation as a string."
  [op]
  (str (:process op)         \tab
       (:type op)            \tab
       (pr-str (:f op))      \tab
       (pr-str (:value op))))

(defn print-history
  "Prints a history to the console."
  [history]
  (doseq [op history]
    (println (op->str op))))

(defn log-op
  "Logs an operation."
  [op]
  (info (op->str op)))

(def logger (agent nil))
(defn log-print
      [_ & things]
      (apply println things))
(defn log
      [& things]
      (apply send-off logger log-print things))

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
  "A reference point for measuring time in a test run.")

(defmacro with-relative-time
  "Binds *relative-time-origin* at the start of body."
  [& body]
  `(binding [*relative-time-origin* (linear-time-nanos)]
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

(defmacro unwrap-exception
  "Catches exceptions from body and re-throws their causes. Useful when you
  don't want the wrapper from, say, a future's exception handler."
  [& body]
  `(try ~@body
        (catch Throwable t#
          (throw (.getCause t#)))))

(defmacro timeout
  "Times out body after n millis, returning timeout-val."
  [millis timeout-val & body]
  `(let [thread# (promise)
         worker# (future
                   (deliver thread# (Thread/currentThread))
                   ~@body)
         retval# (unwrap-exception
                   (deref worker# ~millis ::timeout))]
     (if (= retval# ::timeout)
       (do ; Can never remember which does which
           (.interrupt @thread#)
           (future-cancel worker#)
           ~timeout-val)
       retval#)))

(defn map-kv
  "Takes a function (f [k v]) which returns [k v], and builds a new map by
  applying f to every pair."
  [f m]
  (into {} (r/map f m)))

(defn map-vals
  "Maps values in a map."
  [f m]
  (map-kv (fn [[k v]] [k (f v)]) m))

(defn integer-interval-set-str
  "Takes a set of integers and yields a sorted, compact string representation."
  [set]
  (assert (not-any? nil? set))
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
              "}")))

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
