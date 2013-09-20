(ns jepsen.set-app
  (:require clojure.stacktrace
            [jepsen.control :as control]
            [jepsen.control.net :as control.net]
            [jepsen.console :as console]
            [jepsen.failure :as failure]
            [clojure.java.io :as io])
  (:use jepsen.load
        jepsen.util)
  (:use [clojure.set :only [union difference]]))

(defprotocol SetApp
  (setup [app])
  (add [app element] "Add an element to the set.")
  (results [app] "Return the current set.")
  (teardown [app]))

(def global-lock "hi")
(defn locking-app
  "Wraps an app with a perfect mutex."
  [app]
  (reify SetApp
    (setup    [this]         (locking global-lock (setup app)))
    (add      [this element] (locking global-lock (add app element)))
    (results  [this]         (locking global-lock (results app)))
    (teardown [this]         (locking global-lock (teardown app)))))

(def nodes
  ["n1" "n2" "n3" "n4" "n5"])

(def partitions
  [#{"n1" "n2"}
   #{"n3" "n4" "n5"}])

(defn partition-peers
  "Given a node, returns the nodes visible to that node when partitioned."
  [node]
  (first (filter #(get % node) partitions)))


(defn print-list
  "Tell us a bit about a list of things."
  [coll]
  (if (< (count coll) 16)
    (println coll)
    (apply println (concat (take 6 coll) ["..."] (take-last 6 coll)))))

(defn print-results
  [target acked results]
  (let [target (set target)
        acked (set acked)
        results (set results)]

    (println)
    (println (count target) "total")
    (println (count acked) "acknowledged")
    (println (count results) "survivors")

    (cond
      (= target results)
      (println "All" (count target) "writes succeeded. :-D")

      (= acked results)
      (println "all" (count acked) "acked writes out of" (count target) "succeeded. :-)")

      (empty? target)
      (println "Expected an empty set, but results had"
               (count results)
               "elements!")

      (empty? results)
      (println "Expected"
               (count target)
               "elements, but results were empty!")

      :else
      (do
        (let [diff1 (sort (difference acked results))
              diff2 (sort (difference results acked))]
          (when-not (empty? diff1)
            (println (count diff1) "acknowledged writes lost! (╯°□°）╯︵ ┻━┻")
            (print-list diff1))

            (when-not (empty? diff2)
              (println (count diff2) "unacknowledged writes found! ヽ(´ー｀)ノ")
              (print-list diff2))

            (println (float (/ (count acked) (count target))) "ack rate")
            (println (float (/ (count diff1) (count acked))) "loss rate")
            (println (float (/ (count diff2) (count acked)))
                     "unacknowledged but successful rate"))))))

(defn partition-rr
  "Partitions a sequence into n evenly distributed subsequences."
  [n s]
  (map (fn [offset]
         (keep-indexed (fn [i x]
                         (when (zero? (mod (- i offset) n))
                           x))
                       s))
       (range n)))

(defn worker
  "Runs a workload in a new future. Returns a log of all the write operations."
  [r app workload]
  (future
    (map-fixed-rate r
                    (->> app
                         (partial add)
                         wrap-catch
                         wrap-latency
                         wrap-record-req
                         console/wrap-ordered-log)
                    workload)))

(defn apps
  "Returns a set of apps for testing, given a function."
  [app-fn]
  (map #(app-fn {:host %
                 :hosts (partition-peers %)})
       nodes))

(defn run [r n failure-mode apps]
  ; Set up apps
  (control/on-many nodes (control.net/heal))
  ; Destroys nuodb
  (dorun (map setup apps))

  ; Divide work and start workers
  (let [t0 (System/currentTimeMillis)
        elements (range n)
        duration (/ n r (count nodes))
        _ (log "Run will take" duration "seconds")
        witch (failure/schedule! failure-mode
                                 nodes
                                 (min 10 (* 1/4 duration))
                                 (* 1/2 duration))
        workloads (partition-rr (count apps) elements)
        log (->> (map (partial worker r) apps workloads)
                 doall
                 (mapcat deref)
                 (sort-by :req))
        acked (->> log
                   (remove nil?)
                   (remove #(= :error (:state %)))
                   (map :req))
        t1 (System/currentTimeMillis)]

    ; Spit out logfile
    (with-open [w (io/writer "log.txt")]
      (doseq [l log]
        (.write w (console/line l))
        (.write w "\n")))

    (println (count (filter nil? log)) "unrecoverable timeouts")

    ; Wait for recovery to complete
    @witch

    
    ; Get results
;   (println "Hit enter when ready to collect results.")
;   (read-line0)
;   (sleep 10000)

    (println "Collecting results.")
    (let [results (results (first apps))]
      (println "Writes completed in" (float (/ (- t1 t0) 1000)) "seconds")
      (print-results elements acked results)

      ; Shut down apps
      (dorun (map teardown apps))
      results
      nil)))
