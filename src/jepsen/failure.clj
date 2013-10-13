(ns jepsen.failure
  (:use jepsen.util)
  (:require [jepsen.control :as control]
            [jepsen.control.net :as net]))

(defprotocol Failure
  (fail [mode nodes] "Initiates a failure")
  (recover [mode nodes] "Heals a failure."))

(defn schedule!
  "Schedules a failure to occur after t1 seconds, recovered at t2 seconds."
  [mode nodes t1 t2]
  (future
    (try
      (Thread/sleep (* 1000 t1))
      (fail mode nodes)
      (Thread/sleep (* 1000 (- t2 t1)))
      (recover mode nodes)
      (catch Throwable t
        (log :scheduled-failure
             (with-out-str
               (.printStackTrace t)))))))

(def simple-partition
  "Isolates n1 and n2 from n3, n4, and n5."
  (reify Failure
    (fail [_ nodes]
      (control/on-many nodes (net/partition))
      (log "Partitioned."))

    (recover [_ nodes]
      (control/on-many nodes (net/heal))
      (log "Partition healed."))))

(def noop
  "Does nothing."
  (reify Failure
    (fail [_ _])
    (recover [_ _])))

(defn chaos []
  (let [running (promise)
        done    (promise)]
    (reify Failure
      (fail [_ nodes]
        (future
          (loop []
            (if (deref running 1000 true)
              (do
                (control/on (rand-nth nodes)
                            (net/heal)
                            (net/cut-random-link nodes)
                            (log (control/exec :hostname) (net/iptables-list)))
                (recur))
              (deliver done true)))))
    
      (recover [_ nodes]
        (log "Recovery initiated")
        (deliver running false)
        @done
        (log "Chaos ended")
        (control/on-many nodes (net/heal)) 
        (log "Partition healed.")))))
