(ns jepsen.load
  (:use clojure.tools.logging)
  (:import (java.util.concurrent ScheduledThreadPoolExecutor
                                 ThreadPoolExecutor
                                 SynchronousQueue
                                 TimeUnit)))

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
                            (warn t "map-fixed-rate execution failed"))))))
          ; Done enqueuing
          (deliver done true)))
      0
      (int (/ 1000000 rate))
      TimeUnit/MICROSECONDS)

    ; Await all stasks started
    @done
    (prn "enqueue done")
    (prn :core-pool (.getCorePoolSize executor))
    (prn :active (.getActiveCount executor))
    (prn :completed (.getCompletedTaskCount executor))
    (prn :largest-pool-size (.getLargestPoolSize executor))

    ; Shut down workers
    (.shutdown boss)
    (.shutdown executor)

    ; Await completion
    (if (.awaitTermination executor 30 TimeUnit/SECONDS)
      @output
      (do
        (warn "Timed out waiting for some tasks to complete!")
        @output))))
