(ns jepsen.generator
  "Generates operations for a test. Generators are composable, stateful objects
  which emit operations for processes until they are exhausted, at which point
  they return nil. Generators may sleep when generating operations, to delay
  the rate at which the test proceeds

  Generators do *not* have to emit a :process for their operations; test
  workers will take care of that.

  Big ol box of monads, really."
  (:refer-clojure :exclude [concat delay])
  (:require [jepsen.util :as util])
  (:import (java.util.concurrent.atomic AtomicBoolean)
           (java.util.concurrent CyclicBarrier)))

(defprotocol Generator
  (op [gen test process] "Yields an operation to apply."))

(def void
  "A generator which terminates immediately"
  (reify Generator
    (op [gen test process])))

(defn once
  "A generator which emits a single operation."
  [operation]
  (let [emitted (AtomicBoolean. false)]
    (reify Generator
      (op [gen test process]
        (when-not (.get emitted)
          (when-not (.getAndSet emitted true)
            operation))))))

(defn each
  "Emits a single operation once to each process."
  [op]
  (let [emitted (atom {})]
    (reify Generator
      (op [gen test process]
        (let [emitted (swap! emitted
                             #(assoc % process (inc (get % process 0))))]
          (when (= 1 (get emitted process))
            ; First time!
            op))))))

(defn start-stop
  "A generator which emits a start after a t1 second delay, and then a stop
  after a t2 second delay."
  [t1 t2]
  (let [state (atom :init)]
    (reify Generator
      (op [gen test process]
        (case (swap! state {:init  :start
                            :start :stop
                            :stop  :dead})
          :start (do (Thread/sleep (* t1 1000))
                     {:type :info :f :start})
          :stop  (do (Thread/sleep (* t2 1000))
                     {:type :info :f :stop})
          :dead  nil)))))

(def cas
  "Random cas/read ops for a compare-and-set register over a small field of
  integers."
  (reify Generator
    (op [generator test process]
      (if (< 0.5 (rand))
        {:type  :invoke
         :f     :read}
        {:type  :invoke
         :f     :cas
         :value [(rand-int 5) (rand-int 5)]}))))

(defn queue
  "A random mix of enqueue/dequeue operations over consecutive integers."
  []
  (let [i (atom -1)]
    (reify Generator
      (op [gen test process]
        (if (< 0.5 (rand))
          {:type  :invoke
           :f     :enqueue
           :value (swap! i inc)}
          {:type  :invoke
           :f     :dequeue})))))

(defn drain-queue
  "Wraps a generator, and keeps track of the balance of :enqueue and :dequeue
  operations that pass through. When the underlying generator is exhausted,
  emits enough :dequeue operations to dequeue every attempted enqueue."
  [gen]
  (let [outstanding (atom 0)]
    (reify Generator
      (op [_ test process]
        (if-let [op (op gen test process)]
          (do (when (= :enqueue (:f op))
                (swap! outstanding inc))
              op)

          ; Exhausted
          (when (pos? (swap! outstanding dec))
            {:type :invoke :f :dequeue}))))))

(defn limit
  "Takes a generator and returns a generator which only produces n operations."
  [n gen]
  (let [life (atom (inc n))]
    (reify Generator
      (op [_ test process]
        (when (pos? (swap! life dec))
          (op gen test process))))))

(defn time-limit
  "Yields operations from the underlying generator until dt seconds have
  elapsed."
  [dt source]
  (let [t (atom nil)]
    (reify Generator
      (op [_ test process]
        (when (nil? @t)
          (compare-and-set! t nil (+ (util/linear-time-nanos)
                                     (util/secs->nanos dt))))
        (when (<= (util/linear-time-nanos) @t)
          (op source test process))))))

(defn nemesis
  "Combines a generator of normal operations and a generator for nemesis
  operations into one. When the process requesting an operation is :nemesis,
  routes to the nemesis generator; otherwise to the normal generator."
  [nemesis-gen client-gen]
  (reify Generator
    (op [gen test process]
      (if (= :nemesis process)
        (op nemesis-gen test process)
        (op client-gen test process)))))

(defn synchronize
  "Blocks until all nodes are blocked awaiting operations from this generator,
  then allows them to proceed.

  TODO: ignore nemesis here? Delicate dependency on number of threads :/"
  [gen]
  (let [state (atom :fresh)]
    (reify Generator
      (op [_ test process]
        (when (not= :clear @state)
          ; Ensure a barrier exists
          (compare-and-set! state :fresh
                            (CyclicBarrier. (count (:nodes test))
                                            (partial reset! state :clear)))

          ; Block on barrier
          (.await ^CyclicBarrier @state))
        (op gen test process)))))

(defn concat
  "Takes N generators and constructs a generator that emits operations from the
  first, then the second, then the third, and so on."
  [& generators]
  (let [generators (atom (seq generators))]
    (reify Generator
      (op [gen test process]
        (when-let [gens @generators]
          ; We've got generators to work with
          (if-let [op (op (first gens) test process)]
            op
            ; Whoops, out of ops there; drop first generator and retry
            (do (compare-and-set! generators gens (next gens))
                (recur test process))))))))

(defn phases
  "Like concat, but requires that all threads finish the first generator before
  moving to the second, and so on."
  [& generators]
  (apply concat (map synchronize generators)))

(defn delay
  "Every operation from the underlying generator takes dt seconds to return."
  [dt gen]
  (reify Generator
    (op [_ test process]
      (Thread/sleep (* 1000 dt))
      (op gen test process))))

(defn singlethreaded
  "Obtaining an operation from the underlying generator requires an exclusive
  lock."
  [gen]
  (reify Generator
    (op [this test process]
      (locking this
        (op gen test process)))))

(defn sleep
  "Takes dt seconds, and always produces a nil."
  [dt]
  (delay dt void))

(defn then
  "Generator B, synchronize, then generator A. Why is this backwards? Because
  it reads better in ->> composition."
  [a b]
  (concat b (synchronize a)))

(defn barrier
  "When the given generator completes, synchronizes, then yields nil."
  [gen]
  (->> gen (then void)))
