(ns jepsen.cockroach.nemesis
  "Nemeses for CockroachDB"
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :as util]]
            [jepsen.cockroach.auto :as auto]
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

(defn restarting
  "Wraps a nemesis. After underlying nemesis has completed :stop, restarts
  nodes."
  [nem]
  (reify client/Client
    (setup! [this test node]
      (client/setup! nem test node)
      this)

    (invoke! [this test op]
      (let [op' (client/invoke! nem test op)]
        (if (= :stop (:f op))
          (let [stop (c/on-nodes test (fn [test node]
                                        (try
                                          (auto/start! test node)
                                          :started
                                          (catch RuntimeException e
                                            (.getMessage e)))))]
            (assoc op' :value [(:value op') stop]))
          op')))

    (teardown! [this test]
      (client/teardown! nem test))))

(defn bump-time
  "On randomly selected nodes, adjust the system clock by dt seconds.  Uses
  millisecond precision.  Restarts the db server if it stops."
  [dt]
  (restarting
    (reify client/Client
      (setup! [this test _]
        (auto/reset-clocks! test)
        this)

      (invoke! [this test op]
        (assoc op :value
               (case (:f op)
                 :start (c/with-test-nodes test
                          (if (< (rand) 0.5)
                            (do (c/su (c/exec "/opt/jepsen/bumptime"
                                              (* 1000 dt)))
                                dt)
                            0))
                 :stop (c/with-test-nodes test
                         (auto/reset-clock!)))))

      (teardown! [this test]
        (auto/reset-clocks! test)))))

(defn skew
  "A skew nemesis"
  [name offset]
  (merge nemesis-single-gen
         {:name   name
          :client (bump-time offset)
          :clocks true}))

(def small-skews        (skew "small-skews"       0.100))
(def subcritical-skews  (skew "subcritical-skews" 0.200))
(def critical-skews     (skew "critical-skews"    0.250))
(def big-skews          (skew "big-skews"         0.5))
(def huge-skews         (skew "huge-skews"        60))
