(ns jepsen.control.util
  "Utility functions for scripting installations."
  (:require [jepsen.control :refer :all]
            [clojure.java.io :refer [file]]
            [clojure.string :as str]))

(defn file?
  "Is a file present?"
  [filename]
  (try (exec :stat filename)
       true
       (catch RuntimeException _ false)))

(defn ls
  "A seq of directory entries (not including . and ..). TODO: escaping for
  control chars in filenames (if you do this, WHO ARE YOU???)"
  [dir]
  (->> (str/split (exec :ls :-A dir) #"\n")
       (remove str/blank?)))

(defn ls-full
  "Like ls, but prepends dir to each entry."
  [dir]
  (let [dir (if (re-find #"/$" dir)
              dir
              (str dir "/"))]
    (->> dir
         ls
         (map (partial str dir)))))

(defn wget!
  "Downloads a string URL and returns the filename as a string. Skips if the
  file already exists."
  [url]
  (let [filename (.getName (file url))]
    (when-not (file? filename)
      (exec :wget url))
    filename))

(defn grepkill
  "Kills processes by grepping for the given string."
  ([pattern]
   (grepkill 9 pattern))
  ([signal pattern]
   (exec :ps :aux
         | :grep pattern
         | :grep :-v "grep"
         | :awk "{print $2}"
         | :xargs :kill (str "-" signal))))
