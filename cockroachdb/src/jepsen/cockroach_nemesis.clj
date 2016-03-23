(ns jepsen.cockroach-nemesis
  "Nemeses for CockroachDB"
  (:require
    [jepsen
     [client :as client]
     [control :as c]
     [nemesis :as nemesis]
     [generator :as gen]
     [util :as util]])
  )

;; duration of 1 jepsen test
(def test-duration 30) ; seconds

;; duration between interruptions
(def nemesis-delay 5) ; seconds

;; duration of an interruption
(def nemesis-duration 5) ; seconds

;; duration to let nemeses settle at the end
(def nemesis-quiescence-wait 3) ; seconds

;; Location of the custom utility compiled from scripts/adjtime.c
(def adjtime "/home/ubuntu/adjtime")

;; NTP server to use with `ntpdate`
(def ntpserver "ntp.apple.com")

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

(defn compose-nemesis-clients
  ;; Our own version of nemesis/compose because Jepsen's is broken
  "Takes a map of fs to nemeses and returns a single nemesis which, depending
  on (:f op), routes to the appropriate child nemesis. "
  [nemeses]
  (assert (map? nemeses))
  (reify client/Client
    (setup! [this test node]
      (compose-nemesis-clients (util/map-vals #(client/setup! % test node) nemeses)))

    (invoke! [this test op]
      (let [f (:f op)]
        (loop [nemeses nemeses]
          (if-not (seq nemeses)
            (throw (IllegalArgumentException.
                     (str "no nemesis can handle " (:f op))))
            (let [[fs nemesis] (first nemeses)]
              (if-let [f' (fs f)]
                ;; need to re-assoc on return because jepsen/run asserts :f is preserved.
                (assoc (client/invoke! nemesis test (assoc op :f f')) :f f)
                (recur (next nemeses))))))))

    (teardown! [this test]
      (util/map-vals #(client/teardown! % test) nemeses))))

(defn compose
  [n1 n2]
  {:name (str (:name n1) "-" (:name n2))
   :generator nemesis-double-gen
   :clocks (or (:clocks n1) (:clocks n2))
   :client (compose-nemesis-clients {{:start1 :start, 
                                      :stop1 :stop} (:client n1)
                                     {:start2 :start,
                                      :stop2 :stop} (:client n2)})})

(defn with-nemesis
  "Wraps a client generator in a nemesis that induces failures and eventually
  stops."
  [nemesis-gen client]
  (gen/phases
   (->> client
        (gen/nemesis (:during nemesis-gen))
        (gen/time-limit test-duration))
   (gen/nemesis (:final nemesis-gen))
   (gen/log "waiting for quiescence")
   (gen/sleep nemesis-quiescence-wait)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;; Nemesis definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; empty nemesis
(def none
  {:name "blank"
   :generator nemesis-no-gen
   :client nemesis/noop
   :clocks false
   })   

;; random partitions
(def parts
  {:name "parts"
   :generator nemesis-single-gen
   :client (nemesis/partition-random-halves)
   :clocks false})

;; start/stop server
(defn startstop
  [n]
  {:name (str "startstop" (if (> n 1) n ""))
   :generator nemesis-single-gen
   :client (nemesis/hammer-time (comp (partial take n) shuffle) "cockroach")
   :clocks false})

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
  {:name (str "startkill" (if (> n 1) n ""))
   :generator nemesis-single-gen
   :client (startkill-client n)
   :clocks false})

;; majorities ring
(def majring
  {:name "majring"
   :generator nemesis-single-gen
   :client (nemesis/partition-majorities-ring)
   :clocks false})

;; Clock skew nemesis

(defn clock-milli-scrambler
  "Randomizes the system clock of all nodes within a dt-millisecond window."
  [dt]
  (reify client/Client
    (setup! [this test _]
      this)

    (invoke! [this test op]
      (assoc op :value
             (c/on-many (:nodes test)
                        (c/su
                         (c/exec adjtime (str (- (rand-int (* 2 dt)) dt)))))))

    (teardown! [this test]
      (c/on-many (:nodes test)
                 (c/su (c/exec :ntpdate ntpserver))))
    ))

(def skews
  {:name "skews"
   :generator nemesis-single-gen
   :client (clock-milli-scrambler 100)
   :clocks true})

(def bigskews
  {:name "bigskews"
   :generator nemesis-single-gen
   :client (nemesis/clock-scrambler 600)
   :clocks true})

