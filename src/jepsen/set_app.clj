(ns jepsen.set-app
  (:require clojure.stacktrace)
  (:use [clojure.set :only [union difference]]))

(defprotocol SetApp
  (setup [app])
  (add [app element] "Add an element to the set.")
  (results [app] "Return the current set.")
  (teardown [app]))

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

(def logger (agent nil))
(defn log-print
    [_ & things]
    (apply println things))
(defn log
    [& things]
    (apply send-off logger log-print things))

(defn worker
  "Runs a workload in a new future. Returns a vector of written elements."
  [app workload]
  (future
    (doall
      (remove #{::failed}
              (map-indexed (fn [i element]
                             (try
                               (add app element)
                               (log (str element "\t" :ok))
                               element
                               (catch Throwable e
                                 (log (str element "\t" (.getMessage e)))
                                 (Thread/sleep 1000)
                                 ::failed)))
                           workload)))))

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

(defn apps
  "Returns a set of apps for testing, given a function."
  [app-fn]
  (map #(app-fn {:host %
                 :hosts (partition-peers %)})
       nodes))

(defn run [n apps]
  ; Set up apps
  (dorun (map setup apps))

  ; Divide work and start workers
  (let [t0 (System/currentTimeMillis)
        elements (range n)
        workloads (partition-rr (count apps) elements)
        workers (doall (map worker apps workloads))
   
        ; Wait for workers.
        acked (apply union (map (comp set deref) workers))
        t1 (System/currentTimeMillis)]

    ; Get results
    (println "Hit enter when ready to collect results.")
    (read-line)
    
    (let [results (results (first apps))]
      (println "Writes completed in" (float (/ (- t1 t0) 1000)) "seconds")
      (print-results elements acked results)

      ; Shut down apps
      (dorun (map teardown apps))
      results
      nil)))
