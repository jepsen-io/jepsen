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
    (Thread/sleep (* 1000 t1))
    (fail mode nodes)
    (Thread/sleep (* 1000 (- t2 t1)))
    (recover mode nodes)))

(def simple-partition
  (reify Failure
    (fail [_ nodes]
      (control/on-many nodes (net/partition))
      (log "Partitioned."))

    (recover [_ nodes]
      (control/on-many nodes (net/heal))
      (log "Partition healed."))))

(def noop
  (reify Failure
    (fail [_ _])
    (recover [_ _])))
