(ns jepsen.control.util
  "Utility functions for scripting installations."
  (:require [jepsen.control :refer :all]
            [clojure.java.io :refer [file]]))

(defn file?
  "Is a file present?"
  [filename]
  (try (exec :stat filename)
       true
       (catch RuntimeException _ false)))

(defn wget!
  "Downloads a string URL and returns the filename as a string. Skips if the
  file already exists."
  [url]
  (let [filename (.getName (file url))]
    (when-not (file? filename)
      (exec :wget url))
    filename))

