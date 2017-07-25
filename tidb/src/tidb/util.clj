(ns tidb.util
  (:require [jepsen.control :as c])
)

(defn sql!
  "Execute a mysql string from the command line."
  [s]
  (c/exec :mysql :-h c/*host* :-P "4000" :-u "root" :-e s)
)
