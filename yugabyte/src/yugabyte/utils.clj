(ns yugabyte.utils
  "General helper utility functions"
  (:import (java.util Date)
           (java.text SimpleDateFormat)))

(defn map-values
  "Returns a map with values transformed by function f"
  [m f]
  (reduce-kv (fn [m k v] (assoc m k (f v)))
             {}
             m))

(defn pretty-datetime
  "Pretty-prints given datetime as yyyy-MM-dd_HH:mm:ss.SSS"
  [dt]
  (let [dtf (SimpleDateFormat. "yyyy-MM-dd_HH:mm:ss.SSS")]
    (.format dtf dt)))

(defn current-pretty-datetime
  []
  (pretty-datetime (Date.)))

(defn is-test-geo-partitioned?
  [test]
  (clojure.string/includes? (name (:workload test)) "geo."))

(defn is-test-read-committed?
  [test]
  (clojure.string/includes? (name (:workload test)) "rc."))

(defn is-test-has-pessimistic-locs?
  [test]
  (clojure.string/includes? (name (:workload test)) "pl."))
