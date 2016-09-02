(ns jepsen.nemesis.time
  "Functions for messing with time and clocks."
  (:require [jepsen.control :as c]
            [clojure.java.io :as io])
  (:import (java.io File)))

(defn install!
  "Uploads and compiles some C programs for messing with clocks."
  []
  (c/su
    ; Write out resource to file
    (let [tmp-file (File/createTempFile "jepsen-strobe-time" ".c")]
      (try
        (with-open [r (io/reader (io/resource "strobe-time.c"))]
          (io/copy r tmp-file))
        ; Upload
        (c/exec :mkdir :-p "/opt/jepsen")
        (c/exec :chmod "a+rwx" "/opt/jepsen")
        (c/upload (.getCanonicalPath tmp-file) "/opt/jepsen/strobe-time.c")
        (c/cd "/opt/jepsen"
              (c/exec :gcc "strobe-time.c")
              (c/exec :mv "a.out" "strobe-time"))
        (finally
          (.delete tmp-file))))))

(defn strobe-time!
  "Strobes the time back and forth by delta milliseconds, every period
  milliseconds, for duration seconds."
  [delta period duration]
  (c/su (c/exec "/opt/jepsen/strobe-time" delta period duration)))
