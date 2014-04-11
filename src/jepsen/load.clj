(ns jepsen.load
  (:use jepsen.util)
  (:require [clojure.stacktrace :as trace])
  (:import (java.util.concurrent ScheduledThreadPoolExecutor
                                 ThreadPoolExecutor
                                 SynchronousQueue
                                 TimeUnit)))

(def ok
  "An acknowledged response."
  {:state :ok})

(def error
  "An error response."
  {:state :error})

(def max-threads 100000)

(defn map-fixed-rate
  "Invokes (f element) on each element of a collection, beginning immediately,
  and producing a sequence of outputs. Rate is the number of invocations of f
  per second."
  [rate f coll]
  (let [output   (-> coll count (repeat nil) vec atom)
        executor (doto (ThreadPoolExecutor.
                         1
                         max-threads
                         (/ 2000000 rate)
                         TimeUnit/MICROSECONDS 
                         (SynchronousQueue.))
                   (.allowCoreThreadTimeOut false)
                   (.prestartAllCoreThreads))
        boss     (ScheduledThreadPoolExecutor. 1)
        ; Zip indices into our collection, and add an (immediately discarded)
        ; fencepost element. Yay mutable state.
        coll     (->> coll
                      (map-indexed list)
                      (cons nil)
                      atom)
        done     (promise)]
  
    ; Boss enqueues items regularly
    (.scheduleAtFixedRate
      boss
      (fn boss []
        (if-let [coll (swap! coll next)]
          (let [[i element] (first coll)]
            (.execute executor
                      (fn []
                        (try
                          ; Call function and record result
                          (swap! output assoc i (f element))
                          (catch Throwable t
                            (log "map-fixed-rate execution failed\n")
                            (.printStackTrace t))))))
          ; Done enqueuing
          (deliver done true)))
      0
      (int (/ 1000000 rate))
      TimeUnit/MICROSECONDS)

    ; Await all stasks started
    @done
;    (prn :core-pool (.getCorePoolSize executor))
;    (prn :active (.getActiveCount executor))
;    (prn :completed (.getCompletedTaskCount executor))
;    (prn :largest-pool-size (.getLargestPoolSize executor))

    ; Shut down workers
    (.shutdown boss)
    (.shutdown executor)

    ; Await completion
    (if (.awaitTermination executor 100 TimeUnit/SECONDS)
      @output
      (do
        (log "Timed out waiting for some tasks to complete!")
        @output))))

(defn wrap-latency
  "Returns a function which takes an argument and calls (f arg). (f arg) should
  return a map. Wrap-time will add a new key :latency to this map, which is the
  number of milliseconds the call took."
  [f]
  (fn measure-latency [req]
    (let [t1 (System/nanoTime)
          v  (f req)
          t2 (System/nanoTime)]
      (assoc v :latency (/ (double (- t2 t1)) 1000000.0)))))

(defn wrap-catch
  "Catches any errors thrown by f, logs them, and returns {:state :error
  :message ...}"
  [f]
  (fn catcher [req]
    (try
      (f req)
      (catch Throwable t
        {:state :error
         :message (str (.getMessage t) "\n"
                       (with-out-str
                         (trace/print-cause-trace t)))}))))

(defn wrap-log
  "Returns a function that calls (f req), then logs the req and return value."
  [f]
  (fn logger [req]
    (let [r (f req)]
      (log (str req "\t" (pr-str r)))
      r)))

(defn wrap-record-req
  "Returns a function that calls (f req), and assoc's :req req onto the result."
  [f]
  (fn record-req [req]
    (assoc (f req) :req req)))
