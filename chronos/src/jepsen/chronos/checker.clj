(ns jepsen.chronos.checker
  "We have a sequence of actual runs, and need to verify that they satisfy the
  *expected* schedule of runs. This is a little complicated by the fact that a
  task might be executed multiple times, and possibly vary from the target
  invocation time by up to epsilon seconds.

  We also want to determine:

  - How likely are we to miss an execution?
  - How likely are we to execute a task *multiple* times?

  We refer to an actual execution as a run, and an expected execution as a
  target."
  (:require [clj-time.core :as t]
            [clj-time.format :as tf]
            [clj-time.coerce :as tc]
            [loco.core :as l]
            [loco.constraints :refer :all]))

(defn job->targets
  "Given a job and the datetime of a final read, emits a sequence of clj-time
  Intervals for targets that *must* have been begun at the time of the read."
  [read-time job]
  ; Because jobs are allowed to begin up to :epsilon seconds after the target
  ; time, our true cutoff must be :epsilon seconds before the read.
  (let [interval (t/seconds (:interval job))
        epsilon  (t/seconds (:epsilon job))
        finish   (t/minus read-time epsilon)]
    (->> (:start job)
         (iterate #(t/plus % interval))
         (take (:count job))
         (take-while (partial t/after? finish))
         (map #(t/interval % (t/plus % epsilon))))))

(defn time->int
  "We need integers for the loco solver."
  [t]
  (-> t tc/to-long (/ 1000)))

(defn int->time
  "Convert integers back into DateTimes"
  [t]
  (-> t (* 1000) tc/from-long))

(defn solution
  "Given a job, a read time, and a collection of runs, computes a solution to
  the constraint problem of satisfying every target with a run.

  {:valid?     Whether the runs satisfied the targets for this job
   :solution   A map of target intervals to runs which satisfied those intervals
   :extra      Runs which weren't needed to satisfy the requirements"
  [read-time job runs]
  (let [targets (job->targets read-time job)
        ; What times did the job actually run?
        run-times (map (comp time->int :start) runs)

        ; Index variables
        indices (mapv (partial vector :i) (range (count targets)))

        soln (l/solution
               (cons
                 ($distinct indices)

                 ; For each target...
                 (mapcat (fn [i target]
                           [; The target time should fall within the target's
                            ; range
                            ($in [:target i]
                                 ; Gotta fit into integers
                                 (-> target t/start time->int)
                                 (-> target t/end   time->int)
                                 :bounded)

                            ; The index for this target must point to a run
                            ; time
                            ($in [:i i] 0 (count run-times))

                            ; Target time should be equal to some run time,
                            ; identified by this target's index
                            ($= [:target i] ($nth run-times [:i i]))])

                         (range)
                         targets)))]
    ; Transform solution back into datetime space
    (if soln
      {:valid?   true
       :solution (->> targets
                      (map-indexed (fn [i target]
                                     [target (nth runs (get soln [:i i]))]))
                      (into {}))
       :extra    (->> indices
                      (reduce (fn [runs idx]
                                (assoc runs (get soln idx) nil))
                              runs)
                      (remove nil?))}
      {:valid? false
       :solution nil
       :extra    nil})))
