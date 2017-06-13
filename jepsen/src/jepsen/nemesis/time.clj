(ns jepsen.nemesis.time
  "Functions for messing with time and clocks."
  (:require [jepsen.os.debian :as debian]
            [jepsen [util :as util]
                    [client :as client]
                    [control :as c]
                    [generator :as gen]]
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

(defn clock-nemesis
  "Generates a nemesis which manipulates clocks. Accepts three types of
  operations:

      {:f :reset, :value [node1 ...]}

      {:f :strobe, :value {node1 {:delta ms, :period ms, :duration s} ...}}

      {:f :bump, :value {node1 delta-ms ...}}"
  []
  (reify client/Client
    (setup! [nem test _]
      (reset-time! test)
      nem)

    (invoke! [_ test op]
      (case (:f op)
        :reset (c/on-nodes test (:value op) (fn [test node] (reset-time!)))
        :strobe (let [m (:value op)]
                  (c/on-nodes test (keys m)
                            (fn [test node]
                              (let [{:keys [delta period duration]} (get m node)]
                                (strobe-time! delta period duration)))))
        :bump (let [m (:value op)]
                (c/on-nodes test (keys m)
                            (fn [test node]
                              (bump-time! (get m node))))))
      op)

    (teardown! [_ test]
      (reset-time! test))))

(defn reset-gen
  "Randomized reset generator. Performs resets on random subsets of the tests'
  nodes."
  [test process]
  {:type :info, :f :reset, :value (util/random-nonempty-subset (:nodes test))})

(defn bump-gen
  "Randomized clock bump generator. On random subsets of nodes, bumps the clock
  from -262 to +262 seconds, exponentially distributed."
  [test process]
  {:type  :info
   :f     :bump
   :value (zipmap (util/random-nonempty-subset (:nodes test))
                  (repeatedly (fn []
                                (* (rand-nth [-1 1])
                                   (Math/pow 2 (+ 2 (rand 16)))))))})

(defn strobe-gen
  "Randomized clock strobe generator. On random subsets of the test's nodes,
  introduces clock strobes from 4 ms to 262 seconds, with a period of 1 ms to
  1 second, for a duration of 0-32 seconds."
  [test process]
  {:type  :info
   :f     :strobe
   :value (zipmap (util/random-nonempty-subset (:nodes test))
                  (repeatedly (fn []
                                {:delta (Math/pow 2 (+ 2 (rand 16)))
                                 :period (Math/pow 2 (rand 10))
                                 :duration (rand 32)})))})

(defn clock-gen
  "Emits a random schedule of clock skew operations."
  []
  (gen/mix [reset-gen bump-gen strobe-gen]))
