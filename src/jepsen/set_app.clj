(ns jepsen.set-app
  (:use [clojure.set :only [union difference]]))

(defprotocol SetApp
  (setup [app])
  (add [app element] "Add an element to the set.")
  (results [app] "Return the current set.")
  (teardown [app]))

(defn print-results
  [target results]
  (println)
  (cond
    (= target results)
    (println "All writes succeeded. :-)")

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
      (let [diff1 (difference target results)
            diff2 (difference results target)]
        (when-not (empty? diff1)
          (println (count diff1) "elements missing from result set:")
          (prn (sort diff1)))

        (when-not (empty? diff2)
          (println (count diff2) "spurious elements in result set:")
          (prn (sort diff2)))

        (let [frac (/ (+ (count diff1) (count diff2))
                      (count target))]
          (println (float frac) "error rate"))))))

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
  "Runs a workload in a new future."
  [app workload]
  (future
    (dorun
      (map-indexed (fn [i element]
                     (add app element)
                     (when (zero? (mod i 1))
                       (print ".") (flush)))
                   workload))))

(def global-lock "hi")
(defn locking-app
  "Wraps an app with a perfect mutex."
  [app]
  (reify SetApp
    (setup    [this]         (locking global-lock (setup app)))
    (add      [this element] (locking global-lock (add app element)))
    (results  [this]         (locking global-lock (results app)))
    (teardown [this]         (locking global-lock (teardown app)))))

(defn apps
  "Returns a set of apps for testing, given a function."
  [app-fn]
  (map #(app-fn {:host %})
       ["n1" "n2" "n3" "n4" "n5"]))

(defn run [apps]
  ; Set up apps
  (dorun (map setup apps))

  ; Divide work and start workers
  (let [elements (range 10000)
        workloads (partition-rr (count apps) elements)
        workers (doall (map worker apps workloads))]
   
    ; Wait for workers.
    (dorun (map deref workers))

    ; Get results
    (println "Hit enter when ready to collect results.")
    (read-line)
    (let [results (results (first apps))]
      (print-results (set elements) results)

      ; Shut down apps
      (dorun (map teardown apps))
      results
      nil)))
