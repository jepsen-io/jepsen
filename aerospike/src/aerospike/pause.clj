(ns aerospike.pause
  "Pauses a process to lose writes."
  (:require [aerospike.support :as s]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [control :as c]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [net :as net]
                    [util :as util :refer [real-pmap]]]
            [jepsen.nemesis.time :as nt]
            [jepsen.checker.timeline :as timeline]))

(def healthy-delay
  "Milliseconds to wait before pausing a process"
  5000)

(def pause-delay
  "Milliseconds to wait before unpausing"
  30000)

(def masters-limit
  "Maximum number of concurrently paused master nodes"
  1)

(defn next-healthy
  "Moves a state to the next healthy state. Picks a new master and a new set of
  keys."
  [state test]
  (assoc state
         :state   :healthy
         :masters (take masters-limit (shuffle (:nodes test)))
         :keys    (let [k0 (inc (or (peek (:keys state)) -1))]
                    (vec (range k0 (+ k0 (/ (:concurrency test)
                                            (count (:nodes test)))))))))

(defn pause!
  "Pauses a node based on the :pause-mode of a test."
  [test node]
  (info "pause mode" (pr-str (:pause-mode test)))
  (case (:pause-mode test)
    :process (c/su (c/exec :killall :-19 :asd))
    ; This is a terrible hack; if we increase latency, we destroy our
    ; own ssh connection. Therefore, we'll spawn a mini-daemon to restore the
    ; network to normal after pause-delay ms.
    :net     (c/su (c/exec :nohup :bash :-c
                           (str "tc qdisc add dev eth0 root netem delay "
                                pause-delay "ms 1ms distribution normal; "
                                "sleep " (long (Math/ceil
                                                 (/ pause-delay 1000))) "; "
                                "tc qdisc del dev eth0 root")
                           (c/lit "&"))
                   (Thread/sleep 1000) ; Give that a second to take effect
                   )
    ; We're going to bump the clock, and immediately partition the node away,
    ; so that it locally commits writes with a far-future clock, which will not
    ; be replicated to other nodes.
    :clock (do (nt/bump-time! (* 1000 pause-delay))
               (info "Clock bumped by" pause-delay "seconds")
               (c/on-nodes test (remove #{node} (:nodes test))
                           (fn isolate [test n]
                             (net/drop! (:net test) test node n)
                             (info n "snubbed" node)
                             (net/drop! (:net test) test n node)
                             (info node "snubbed" n)))))
  :paused)

(defn resume!
  "Resumes a node based on the :pause-mode of a test, and updates :state."
  [test node]
  (case (:pause-mode test)
    :process (c/su (c/exec :killall :-18 :asd))
    :net     nil ; not applicable
    :clock   (do (nt/reset-time!)
                 (net/heal! (:net test) test)
                 (c/on-nodes test (remove #{node} (:nodes test))
                             (fn restart [test node]
                               (c/su (c/exec :service :aerospike :restart))))))
  :resumed)

(defn nemesis
  [state]
  (reify nemesis/Nemesis
    (setup! [this test]
      (c/with-test-nodes test (nt/install!))
      (nt/reset-time! test)
      this)

    (invoke! [this test op]
      (let [v (c/on-nodes test (:value op)
                                   (fn [test node]
                                     (case (:f op)
                                       :pause  (pause!  test node)
                                       :resume (resume! test node))))]
        (case (:f op)
          :pause  (swap! state assoc :state :paused)
          :resume (swap! state next-healthy test))
        (info :state @state)
        (assoc op :value v)))))

(defrecord Client [namespace set state client]
  client/Client
  (open! [this test node]
    (assoc this :client (s/connect node)))

  (setup! [this test] this)

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (s/with-errors op #{}
        (case (:f op)
          :read (assoc op
                       :type :ok,
                       :value (independent/tuple k
                                (-> client
                                    (s/fetch namespace set k)
                                    :bins
                                    :value
                                    (or "")
                                    (str/split #" ")
                                    (->> (remove str/blank?)
                                         (map #(Long/parseLong %))
                                         (into (sorted-set))))))

          :add (do (try (s/append! client namespace set k {:value (str " " v)}))
                   ; If we're paused, move us to state :wait.
                   (swap! state (fn [s]
                                  (if (= :paused (:state s))
                                    (assoc s :state :wait)
                                    s)))
                   (assoc op :type :ok))))))

  (teardown! [this test])

  (close! [this test]
    (s/close client)))

(defn client [state]
  (Client. s/ans "pause" state nil))

(defn generator
  [state]
  (let [next-value (atom -1)]
    (reify gen/Generator
      (op [this test process]
        (if (= :nemesis process)
          ; Nemesis ops
          (case (:state @state)
            :healthy (do (Thread/sleep healthy-delay)
                         {:type :info, :f :pause, :value (:masters @state)})
            :pausing (assert false "should be unreachable")
            :paused  (do (Thread/sleep 100)
                         (recur test process))
            :wait    (do (Thread/sleep (if (= :clock (:pause-mode test))
                                         0
                                         pause-delay))
                         {:type :info, :f :resume, :value (:masters @state)}))

          ; Client ops
          (if (= :wait (:state @state))
            (do (Thread/sleep 100)
                (recur test process))
            (do {:type  :invoke
                 :f     :add
                 :value (independent/tuple
                          (let [state @state]
                            (nth (:keys state) (mod process (count (:keys state)))))
                          (swap! next-value inc))})))))))

(defn workload+nemesis
  "Hokay, so:

  We're going to:

  - Without loss of generality, pick a node A to interfere with, which is
    currently the master for some key.
  - Begin continuous writes on B, C....
    - These writes will be proxied to A, the master.
  - Pause A. With luck, an in-flight write will be trapped in A's memory.
  - Continue writes on B, C, ...
    - These will fail until a new master (one of B, C, ...) is promoted
  - Once one write succeeds, stop all writes and wait 25+ seconds.
    - Let the final write's timestamp be t1
  - Resume A
    - A will process in-flight writes locally
    - assigning them timestamp t1 + 25
  - Perform a read; the final read will be lost in favor of A.

  What kind of writes will work with this workload? We need one where we can
  tell that we've lost updates. Blind writes are preferable, because CAS
  requires a prior read. We'll use string append with a set workload.

  We're going to model this using a state machine, shared by the nemesis,
  client, and generator. States proceed in a loop, as follows:

  - healthy: writes proceed as normal. After a few seconds, the generator emits
    a nemesis pause op, and moves us to...
  - pausing: writes continue. The nemesis, once complete, moves us to...
  - paused: we continue writes. If our key's master was A, then writes will
    fail until an election promotes another node. The first successful client
    write during :paused moves us to...
  - wait: we cease all operations for 25+ seconds, then the generator picks new
    keys, issues a nemesis resume operation, and moves us to :healthy

  This is an awful hack and is subject to SO many deadlocks"
  [test]
  (let [state (atom (next-healthy {:state :wait} test))
        gen   (generator state)]
    {:workload {:client  (client state)
                :checker (independent/checker (checker/set))
                :generator gen
                :final-generator
                (gen/derefer
                  (delay
                    (locking state
                      (independent/concurrent-generator
                        (count (:nodes test))
                        (range (peek (:keys @state)))
                        (fn [k]
                          (gen/once {:type :invoke
                                     :f    :read}))))))}
     :nemesis {:nemesis           (nemesis state)
               :generator         gen
               :final-generator   (gen/concat
                                    (gen/once
                                      (fn [test process]
                                        {:type  :info
                                         :f     :resume
                                         :value (:nodes test)}))
                                    (gen/sleep 10))}}))
