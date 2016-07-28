(ns jepsen.cockroach.nemesis
  "Nemeses for CockroachDB"
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :as util]]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]))

;; duration between interruptions
(def nemesis-delay 5) ; seconds

;; duration of an interruption
(def nemesis-duration 5) ; seconds

;; duration to let nemeses settle at the end
(def nemesis-quiescence-wait 3) ; seconds

;; Location of the custom utility compiled from scripts/adjtime.c
(def adjtime "/home/ubuntu/adjtime")

;; NTP server to use with `ntpdate`
(def ntpserver "ntp.ubuntu.com")

;;;;;;;;;;;;;;;;;;; Common definitions ;;;;;;;;;;;;;;;;;;;;;;

(def nemesis-no-gen
  {:during gen/void
   :final gen/void})

(def nemesis-single-gen
  {:during (gen/seq (cycle [(gen/sleep nemesis-delay)
                            {:type :info, :f :start}
                            (gen/sleep nemesis-duration)
                            {:type :info, :f :stop}]))
   :final (gen/once {:type :info, :f :stop})})

(def nemesis-double-gen
  {:during (gen/seq (cycle [(gen/sleep nemesis-delay)
                            {:type :info, :f :start1}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :start2}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :stop1}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :stop2}
                            (gen/sleep nemesis-delay)
                            {:type :info, :f :start2}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :start1}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :stop2}
                            (gen/sleep (/ nemesis-duration 2))
                            {:type :info, :f :stop1}
                            ]))
   :final (gen/seq [{:type :info, :f :stop1}
                    {:type :info, :f :stop2}])
   })

(defn compose
  [n1 n2]
  (merge nemesis-double-gen
         {:name (str (:name n1) "-" (:name n2))
          :clocks (or (:clocks n1) (:clocks n2))
          :client (nemesis/compose {{:start1 :start,
                                     :stop1 :stop} (:client n1)
                                    {:start2 :start,
                                     :stop2 :stop} (:client n2)})}))

(defn with-nemesis
  "Wraps a client generator in a nemesis that induces failures and eventually
  stops."
  [nemesis-gen client]
  (->> client
       (gen/nemesis
         (gen/phases (:during nemesis-gen)
                     (:final nemesis-gen)
                     (gen/log "waiting for quiescence")
                     (gen/sleep nemesis-quiescence-wait)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;; Nemesis definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; empty nemesis
(def none
  (merge nemesis-no-gen
         {:name "blank"
          :client nemesis/noop
          :clocks false}))

;; random partitions
(def parts
  {:name "parts"
   :generator nemesis-single-gen
   :client (nemesis/partition-random-halves)
   :clocks false})

;; start/stop server
(defn startstop
  [n]
  (merge nemesis-single-gen
         {:name (str "startstop" (if (> n 1) n ""))
          :client (nemesis/hammer-time
                    (comp (partial take n) shuffle) "cockroach")
          :clocks false}))

;; start/kill server
(defn startkill-client
  [n]
  (nemesis/node-start-stopper (comp (partial take n) shuffle)
                              (fn start [t n]
                                (c/su (c/exec :killall :-9 :cockroach))
                                [:paused :cockroach])
                              (fn stop [t n]
                                (c/su (c/exec (:runcmd t)))
                                [:resumed :cockroach])))

(defn startkill
  [n]
  (merge nemesis-single-gen
         {:name (str "startkill" (if (> n 1) n ""))
          :client (startkill-client n)
          :clocks false}))

;; majorities ring
(def majring
  (merge nemesis-single-gen
         {:name "majring"
          :client (nemesis/partition-majorities-ring)
          :clocks false}))

;; Little convenience macro
(defmacro on-nodes [test & body]
  `(c/on-nodes ~test (fn [test# node#] ~@body)))

;; Clock skew nemesis

(defn reset-clocks!
  "Reset all clocks on all nodes in a test"
  [test]
  (on-nodes test (c/su (c/exec :ntpdate ntpserver))))

(defn clock-milli-scrambler
  "Randomizes the system clock of all nodes within a dt-millisecond window."
  [dt]
  (reify client/Client
    (setup! [this test _]
      (reset-clocks! test)
      this)

    (invoke! [this test op]
      (assoc op :value
             (on-nodes test
                       (c/su
                         (c/exec adjtime (str (- (rand-int (* 2 dt)) dt)))))))

    (teardown! [this test]
      (reset-clocks! test))))

(def skews
  (merge nemesis-single-gen
         {:name "skews"
          :client (clock-milli-scrambler 100)
          :clocks true}))

(defn clock-scrambler-restart
  "Randomizes the system clock of all nodes within a dt-second window.
  Restarts the db server if it stops."
  [dt]
  (reify client/Client
    (setup! [this test _]
      (reset-clocks! test)
      this)

    (invoke! [this test op]
      (assoc op :value
             (case (:f op)
               :start (on-nodes test
                                  (let [t (+ (/ (System/currentTimeMillis) 1000)
                                             (- (rand-int (* 2 dt)) dt))]
                                    (nemesis/set-time! t)))
               :stop (on-nodes test
                                 (c/su (c/exec :ntpdate ntpserver))
                                 (when (= "" (try
                                               (c/exec :ps :a c/|
                                                       :grep :cockroach c/|
                                                       :grep :-v :grep)
                                               (catch RuntimeException e "")))
                                   (try
                                     (c/su (c/exec (:runcmd test)))
                                     (catch RuntimeException e (.getMessage e))))))))


    (teardown! [this test]
      (reset-clocks! test))))

(def bigskews
  (merge nemesis-single-gen
         {:name "bigskews"
          :client (clock-scrambler-restart 60)
          :clocks true}))
