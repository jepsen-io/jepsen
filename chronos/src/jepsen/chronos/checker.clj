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
            [clojure.pprint :refer [pprint]]
            [jepsen.util :as util :refer [meh]]
            [jepsen.checker :refer [Checker]]
            [jepsen.checker.perf :as perf]
            [jepsen.store :as store]
            [loco.core :as l]
            [loco.constraints :refer :all]
            [gnuplot.core :as g]))

(def epsilon-forgiveness
  "We let chronos miss its deadlines by a few seconds."
  5)

(defn job->targets
  "Given a job and the datetime of a final read, emits a sequence of
  [start-time stop-time] for targets that *must* have been begun at the time
  of the read."
  [read-time job]
  ; Because jobs are allowed to begin up to :epsilon seconds after the target
  ; time, our true cutoff must be :epsilon seconds before the read, and because
  ; jobs take :duration seconds to complete, we need an additional :duration
  ; there too.
  (let [interval (t/seconds (:interval job))
        epsilon  (t/seconds (:epsilon job))
        duration (t/seconds (:duration job))
        finish   (t/minus read-time epsilon duration)]
    (->> (:start job)
         (iterate #(t/plus % interval))
         (take (:count job))
         (take-while (partial t/after? finish))
         (map #(vector % (t/plus % epsilon (t/seconds epsilon-forgiveness)))))))

(defn time->int
  "We need integers for the loco solver."
  [t]
  (-> t tc/to-long (/ 1000) int))

(defn int->time
  "Convert integers back into DateTimes"
  [t]
  (-> t (* 1000) tc/from-long))

(defn complete-incomplete-runs
  "Given a sequence of runs, partitions it into a sequence of completed runs
  and a sequence of incomplete runs."
  [runs]
  (loop [complete   (transient [])
         incomplete (transient [])
         runs       (seq runs)]
    (if runs
      (let [r (first runs)]
        (if (:end r)
          (recur (conj! complete r)
                 incomplete
                 (next runs))
          (recur complete
                 (conj! incomplete r)
                 (next runs))))
      [(vec (sort-by :start (persistent! complete)))
       (vec (sort-by :start (persistent! incomplete)))])))

(defn disjoint-job-solution
  "Given sorted lists of targets and runs, computes a sorted map of targets to
  runs, where a satisfying run exists. Throws if targets are not disjoint."
  [targets runs]
  ; Both runs and targets are sorted by their start time, which allows us to
  ; riffle the two together in O(n) time.
  (loop [m       (sorted-map)
         targets targets
         runs    runs]
    (let [target (first targets)
          run   (first runs)]
      ; Safety first
      (when target
        (when-let [target2 (second targets)]
          (assert (t/before? (second target) (first target2)))))

      (cond
        ; If we're out of targets, exit
        (nil? target) m

        ; If we're out of runs, zip through remaining targets
        (nil? run)
        (recur (assoc m target nil) (next targets) (next runs))

        ; This run started before the target began.
        (t/before? (:start run) (first target))
        (recur m targets (next runs))

        ; This run started after the target ended.
        (t/before? (second target) (:start run))
        (recur (assoc m target nil) (next targets) runs)

        ; This run falls into the target.
        true
        (recur (assoc m target run)
               (next targets)
               (next runs))))))

(defn job-solution
  "Given a job, a read time, and a collection of runs, computes a solution to
  the constraint problem of satisfying every target with a run.

  {:valid?     Whether the runs satisfied the targets for this job
   :job        The job itself
   :solution   A sorted map of target intervals to runs which satisfied them
   :extra      Complete runs which weren't needed to satisfy the requirements
   :complete   Runs which did complete
   :incomplete Runs which did began but did not complete"
  [read-time job runs]
  (let [targets (job->targets read-time job)

        ; Split off incomplete runs; they don't count
        [runs incomplete] (complete-incomplete-runs runs)

        ; What times did the job actually run?
        run-times (map (comp time->int :start) runs)

        ; Index variables
        indices (mapv (partial vector :i) (range (count targets)))

;        _ (prn :job job)
;        _ (prn :run-times run-times)
;        _ (prn :incompletes incomplete)
;        _ (prn :targets targets)

        soln (if (empty? targets)
               ; Trivial case--loco will crash if we ask for 0 distinct vars
               {}
               (l/solution
                 (cons
                   ($distinct indices)

                   ; For each target...
                   (mapcat (fn [i [start end]]
                             [; The target time should fall within the target's
                              ; range
                              ($in [:target i]
                                   (time->int start)
                                   (time->int end)
                                   :bounded)

                              ; The index for this target must point to a run
                              ; time
                              ($in [:i i] 0 (count run-times))

                              ; Target time should be equal to some run time,
                              ; identified by this target's index
                              ($= [:target i] ($nth run-times [:i i]))])

                           (range)
                           targets))))]
    ; Transform solution back into datetime space
    (if soln
      {:valid?   true
       :job      job
       :solution (->> targets
                      (map-indexed (fn [i target]
                                     [target (nth runs (get soln [:i i]))]))
                      (into (sorted-map)))
       :extra    (->> indices
                      (reduce (fn [runs idx]
                                (assoc runs (get soln idx) nil))
                              runs)
                      (remove nil?))
       :complete   runs
       :incomplete incomplete}
      {:valid?      false
       :job         job
       :solution    (meh (disjoint-job-solution targets runs))
       :extra       nil
       :complete    runs
       :incomplete  incomplete})))

(defn solution
  "Given a read time, a collection of jobs, and a collection of runs,
  partitions jobs and runs by name, analyzes each one, and emits a map like:

  {:valid?      true iff every job has a valid solution
   :jobs        A map of job names to job solutions, each with :valid?, etc.
   :extra       Runs which weren't needed to satisfy a job's constraints
   :incomplete  Runs which did not complete
   :read-time   Time of the final read}"
  [read-time jobs runs]
  (let [jobs  (group-by :name jobs)
        runs  (group-by :name runs)
        solns (util/map-vals
                (fn [jobs]
                  (assert (= 1 (count jobs)))
                  (let [job (first jobs)]
                    (job-solution read-time job (get runs (:name job)))))
                jobs)]
    {:valid?     (every? :valid? (vals solns))
     :jobs       (into (sorted-map) solns)
     :extra      (mapcat :extra (vals solns))
     :incomplete (mapcat :incomplete (vals solns))
     :read-time  read-time}))

(defn time->secs
  "Converts an absolute time to seconds relative to the given t0."
  [t0 t]
  (/ (t/in-millis (t/interval t0 t)) 1000.0))

(def green "#00AB01")
(def red   "#AB0001")

(defn plot-targets
  "Takes a start time and a [job solution] pair, and computes a sequence of
  gnuplot shaded regions for all targets, colorized based on whether they were
  satisfied."
  [start-time [job solution]]
  (->> solution
       :solution
       (map (fn [[target run]]
              (let [start (time->secs start-time (first target))
                    end   (time->secs start-time (second target))
                    color (if run green red)]
                [:set :obj :rect
                 :from (g/list start (+ 0.1 job))
                 :to   (g/list end (+ 0.9 job))
                 :fillcolor :rgb color
                 :fillstyle :transparent :solid 0.3
                 :noborder])))))

(defn plot-runs
  "Takes a start time and a [job solution] pair, and computes a sequence of
  gnuplot points for all run based on their start times, colorized based on
  whether they completed."
  [start-time [job solution]]
  (->> (concat (:complete solution) (:incomplete solution))
       (map (fn [run]
              (let [start (time->secs start-time (:start run))
                    end   (max (if-let [end (:end run)]
                                 (time->secs start-time (:end run))
                                 0)
                               (+ 1 start)) ; ensure regions are always drawn
                    color (if (:end run) green red)]
                [:set :obj :rect
                 :from (g/list start (+ job 0.4))
                 :to   (g/list end   (+ job 0.6))
                 :fillcolor :rgb color
                 :fillstyle :solid
                 :noborder])))))

(defn plot!
  [test solution]
  "Given a test and a solution, dumps out a plot of targets colorized by
  whether they were fulfilled or not. Returns solution."
  (let [output-path (.getCanonicalPath (store/path! test "chronos.png"))
        start-time  (:start-time test)
        ; Compute a sequence of gnuplot shaded regions for each target
        ; interval.
        targets (->> solution
                     :jobs
                     (mapcat (partial plot-targets start-time)))
        runs    (->> solution
                     :jobs
                     (mapcat (partial plot-runs start-time)))]
    (g/raw-plot! (concat (perf/preamble output-path)
                         [[:set :ylabel "Job"]
                          [:set :key :off]
                          [:set :xrange (g/range 0 (time->secs
                                                    start-time
                                                    (:read-time solution)))]
                          [:set :yrange (g/range 0 (->> solution
                                                       :jobs
                                                       keys
                                                       (reduce max 1)))]]
                         (perf/nemesis-regions (:history test))

                         targets
                         runs
                         [['plot (g/list ["-" 'with 'linespoints 'title "hi"])]])

                 [[]])
    solution))

(defn checker
  "Constructs a Jepsen checker."
  []
  (reify Checker
    (check [_ test model history]
      (let [read-time (->> history
                           rseq
                           ; TODO: make sure invocation and completion
                           ; are from the SAME op
                           (filter #(and (= :invoke (:type %))
                                         (= :read (:f %))))
                           first
                           :time
                           util/nanos->secs
                           t/seconds
                           (t/plus (:start-time test)))
            runs      (->> history
                           rseq
                           (filter #(and (= :ok (:type %))
                                         (= :read (:f %))))
                           first
                           :value)
            jobs      (->> history
                           (filter #(and (= :ok (:type %))
                                         (= :add-job (:f %))))
                           (map :value))]
        (assert runs) ; If we can't find a read, this will be nil.
        (plot! test (solution read-time jobs runs))))))
