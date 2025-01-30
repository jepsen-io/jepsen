(ns jepsen.nemesis.time
  "Functions for messing with time and clocks."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.os.debian :as debian]
            [jepsen.os.centos :as centos]
            [jepsen [util :as util]
                    [client :as client]
                    [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]]
            [jepsen.control.util :as cu]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io File)))

(defn install!
  "Uploads and compiles some C programs for messing with clocks."
  []
  (nemesis/compile-c-resource! "strobe-time.c" "strobe-time")
  (nemesis/compile-c-resource! "bump-time.c" "bump-time"))

(defn parse-time
  "Parses a decimal time in unix seconds since the epoch, provided as a string,
  to a bigdecimal"
  [s]
  (bigdec (str/trim-newline s)))

(defn clock-offset
  "Takes a time in seconds since the epoch, and subtracts the local node time,
  to obtain a relative offset in seconds."
  [remote-time]
  (float (- remote-time (/ (System/currentTimeMillis) 1000.0))))

(defn current-offset
  "Returns the clock offset of this node, in seconds."
  []
  (clock-offset (parse-time (c/exec :date "+%s.%N"))))

(defn maybe-disable-ntp!
  "Tries to turn off any running NTP service. We let these fail
  quietly--there are lots of ways to run (or not run) NTP."
  []
  (c/su
    ; Systemd took over timekeeping so now we also have to go ask systemd
    ; to turn that off. Hilariously, the program to control that does an
    ; RPC call to systemd-timedated, which will then *time out* after 30
    ; seconds if the service is disabled--which is normal in containers.
    ;
    ; root@n1:~# timedatectl status
    ; Failed to query server: Connection timed out
    ;
    ; To work around this, we probe the timedated service first and only
    ; ask it to disable NTP if it's running. I hate everything about this.
    (try+ (c/exec :service :timedated :status)
          ; If this succeeds, we can ask it to stop managing NTP.
          (c/exec :timedatectl :set-ntp :false)
          (catch [:type :jepsen.control/nonzero-exit] _
            ; Service not running
            ))
    ; On older Debian platforms, try to stop ntpd service directly
    (try+ (c/exec :service :ntpd :stop)
          (catch [:type :jepsen.control/nonzero-exit] _))))

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
  "Generates a nemesis which manipulates clocks. Accepts four types of
  operations:

      {:f :reset, :value [node1 ...]}

      {:f :strobe, :value {node1 {:delta ms, :period ms, :duration s} ...}}

      {:f :bump, :value {node1 delta-ms ...}}

      {:f :check-offsets}"
  []
  (reify nemesis/Nemesis
    (setup! [nem test]
      (c/with-test-nodes test
        (install!)
        (maybe-disable-ntp!)
        (try+ (reset-time!)
              (catch [:type :jepsen.control/nonzero-exit, :exit 1] _
                ; Bit awkward: on some platforms, like containers, we *can't*
                ; step the time, but the way nemesis composition works makes it
                ; so that we still get glued into the overall test nemesis even
                ; if we'll never be called. We'll allow this ntpdate to fail
                ; silently--it's just to help when we *do* mess with times.
                )))
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
      (c/with-test-nodes test
        (try+ (reset-time!)
              (catch [:type :jepsen.control/nonzero-exit, :exit 1] _
                ; Bit awkward: on some platforms, like containers, we *can't*
                ; step the time, but the way nemesis composition works makes it
                ; so that we still get glued into the overall test nemesis even
                ; if we'll never be called. We'll allow this ntpdate to fail
                ; silently--it's just to help when we *do* mess with times.
                ))))))

(defn reset-gen-select
  "A function which returns a generator of reset operations. Takes a function
  (select test) which returns nodes from the test we'd like to target for that
  clock reset."
  [select]
  (fn [test process]
    {:type :info, :f :reset, :value (select test)}))

(def reset-gen
  "Randomized reset generator. Performs resets on random subsets of the test's
  nodes."
  (reset-gen-select (comp util/random-nonempty-subset :nodes)))

(defn bump-gen-select
  "A function which returns a clock bump generator that bumps the clock from
  -288 to +288 seconds, exponentially distributed. (select test) is used to
  select which subset of the test's nodes to use as targets in the generator."
  [select]
  (fn gen [test process]
    {:type  :info
     :f     :bump
     :value (zipmap (select test)
                    (repeatedly
                      (fn rand-offset []
                        (long (* (rand-nth [-1 1])
                                 (Math/pow 1.5 (+ 6 (rand 25))))))))}))

(def bump-gen
  "Randomized clock bump generator targeting a random subsets of nodes."
  (bump-gen-select (comp util/random-nonempty-subset :nodes)))

(defn strobe-gen-select
  "A function which returns a clock strobe generator that introduces clock
  strobes from 4 ms to 262 seconds, with a period of 1 ms to 1 second, for a
  duration of 0-32 seconds. (select test) is used to select which subset of the
  test's nodes to use as targets in the generator."
  [select]
  (fn [test process]
    {:type  :info
     :f     :strobe
     :value (zipmap (select test)
                    (repeatedly (fn []
                                  {:delta (long  ;(Math/pow 2 (+ 2 (rand 16))) 
                                           (+ 1 (rand 27000)))
                                   :period (long (Math/pow 2 (rand 10)))
                                   :duration (rand 32)})))}))

(def strobe-gen
  "Randomized clock strobe generator targeting a random subsets of the test's
  nodes."
  (strobe-gen-select (comp util/random-nonempty-subset :nodes)))

(defn clock-gen
  "Emits a random schedule of clock skew operations. Always starts by checking
  the clock offsets to establish an initial bound."
  []
  (gen/phases
    {:type :info, :f :check-offsets}
    (gen/mix [reset-gen bump-gen strobe-gen])))
