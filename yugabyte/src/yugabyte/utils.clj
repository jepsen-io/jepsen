(ns yugabyte.utils
  "General helper utility functions")

(defn map-values
  "Returns a map with values transformed by function f"
  [m f]
  (reduce-kv (fn [m k v] (assoc m k (f v)))
             {}
             m))
