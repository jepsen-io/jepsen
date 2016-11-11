(ns jepsen.nemesis.time
  "Functions for messing with time and clocks."
  (:require [jepsen.control :as c]
            [jepsen.os.debian :as debian]
            [clojure.java.io :as io])
  (:import (java.io File)))

(defn compile!
  "Takes a Reader to C source code and spits out a binary to /opt/jepsen/<bin>."
  [reader bin]
  (c/su
    (let [tmp-file (File/createTempFile "jepsen-upload" ".c")]
      (try
        (io/copy reader tmp-file)
        ; Upload
        (c/exec :mkdir :-p "/opt/jepsen")
        (c/exec :chmod "a+rwx" "/opt/jepsen")
        (c/upload (.getCanonicalPath tmp-file) (str "/opt/jepsen/" bin ".c"))
        (c/cd "/opt/jepsen"
              (c/exec :gcc (str bin ".c"))
              (c/exec :mv "a.out" bin))
        (finally
          (.delete tmp-file)))))
  bin)

(defn compile-resource!
  "Given a resource name, spits out a binary to /opt/jepsen/<bin>."
  [resource bin]
  (with-open [r (io/reader (io/resource resource))]
    (compile! r bin)))

(defn install!
  "Uploads and compiles some C programs for messing with clocks."
  []
  (c/su
    (debian/install [:build-essential])

    (compile-resource! "strobe-time.c" "strobe-time")
    (compile-resource! "bump-time.c" "bump-time")))

(defn reset-time!
  "Resets the local node's clock to NTP. If a test is given, resets time on all
  nodes across the test."
  ([]     (c/su (c/exec :ntpdate :-b "pool.ntp.org")))
  ([test] (c/with-test-nodes test (reset-time!))))

(defn bump-time!
  "Adjusts the clock by delta milliseconds."
  [delta]
  (c/su (c/exec "/opt/jepsen/bump-time" delta)))

(defn strobe-time!
  "Strobes the time back and forth by delta milliseconds, every period
  milliseconds, for duration seconds."
  [delta period duration]
  (c/su (c/exec "/opt/jepsen/strobe-time" delta period duration)))
