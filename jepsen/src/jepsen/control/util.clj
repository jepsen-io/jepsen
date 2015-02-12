(ns jepsen.control.util
  "Utility functions for scripting installations."
  (:use jepsen.control))

(defn file?
  "Is a file present?"
  [filename]
  (try (exec :stat filename)
       true
       (catch RuntimeException _ false)))
