(ns jepsen.nemesis.time
  "Functions for messing with time and clocks."
  (:require [jepsen.os.debian :as debian]
            [jepsen.os.centos :as centos]
            [jepsen [util :as util]
                    [client :as client]
                    [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]]
            [clojure.string :as str]
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

(defn compile-tools!
  []
  (compile-resource! "strobe-time.c" "strobe-time")
  (compile-resource! "bump-time.c" "bump-time"))

(defn install!
  "Uploads and compiles some C programs for messing with clocks."
  []
  (c/su
   (try (compile-tools!)
     (catch RuntimeException e
       (try (debian/install [:build-essential])
         (catch RuntimeException e
           (centos/install [:gcc])))
       (compile-tools!)))))

(defn parse-time
  "Parses a decimal time in unix seconds since the epoch, provided as a string,
  to a bigdecimal"
  [s]
  (bigdec (str/trim-newline s)))

(defn clock-offset
  "Takes a time in seconds since the epoch, and subtracts the local node time,
  to obtain a relative offset in seconds."
  [remote-time]
  (- remote-time (/ (System/currentTimeMillis) 1000)))

(defn current-offset
  "Returns the clock offset of this node, in seconds."
  []
  (clock-offset (parse-time (c/exec :date "+%s.%N"))))

(defn reset-time!
  "Resets the local node's clock to NTP. If a test is given, resets time on all
  nodes across the test."
  ([]     (c/su (c/exec :ntpdate :-b "time.google.com")))
  ([test] (c/with-test-nodes test (reset-time!))))

(defn bump-time!
  "Adjusts the clock by delta milliseconds. Returns the time offset from the
  current local wall clock, in seconds."
  [delta]
  (c/su (clock-offset (parse-time (c/exec "/opt/jepsen/bump-time" delta)))))

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
  (reify nemesis/Nemesis
    (setup! [nem test]
      (c/with-test-nodes test (install!))
      ; Try to stop ntpd service in case it is present and running.
      (c/with-test-nodes test
        (try (c/su (c/exec :service :ntp :stop))
             (catch RuntimeException e))
        (try (c/su (c/exec :service :ntpd :stop))
             (catch RuntimeException e)))
      (reset-time! test)
      nem)

    (invoke! [_ test op]
      (let [res (case (:f op)
                  :reset (c/on-nodes test (:value op) (fn [test node]
                                                        (reset-time!)
                                                        (current-offset)))

                  :check-offsets (c/on-nodes test (fn [test node]
                                                    (current-offset)))

                  :strobe
                  (let [m (:value op)]
                    (c/on-nodes test (keys m)
                                (fn [test node]
                                  (let [{:keys [delta period duration]}
                                        (get m node)]
                                    (strobe-time! delta period duration))
                                  (current-offset))))

                  :bump
                  (let [m (:value op)]
                    (c/on-nodes test (keys m)
                                (fn [test node]
                                  (bump-time! (get m node))))))]
        (assoc op :clock-offsets res)))

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
                                (long (* (rand-nth [-1 1])
                                         (Math/pow 2 (+ 2 (rand 16))))))))})

(defn strobe-gen
  "Randomized clock strobe generator. On random subsets of the test's nodes,
  introduces clock strobes from 4 ms to 262 seconds, with a period of 1 ms to
  1 second, for a duration of 0-32 seconds."
  [test process]
  {:type  :info
   :f     :strobe
   :value (zipmap (util/random-nonempty-subset (:nodes test))
                  (repeatedly (fn []
                                {:delta (long (Math/pow 2 (+ 2 (rand 16))))
                                 :period (long (Math/pow 2 (rand 10)))
                                 :duration (rand 32)})))})

(defn clock-gen
  "Emits a random schedule of clock skew operations. Always starts by checking
  the clock offsets to establish an initial bound."
  []
  (gen/phases
    (gen/once {:type :info, :f :check-offsets})
    (gen/mix [reset-gen bump-gen strobe-gen])))
