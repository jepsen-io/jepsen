(ns jepsen.generator
  "Generates operations for a test. Generators are composable, stateful objects
  which emit operations for processes until they are exhausted, at which point
  they return nil. Generators may sleep when generating operations, to delay
  the rate at which the test proceeds

  Generators do *not* have to emit a :process for their operations; test
  workers will take care of that.

  Every object may act as a generator, and constantly yields itself.

  Big ol box of monads, really."
  (:refer-clojure :exclude [concat delay seq filter await])
  (:require [jepsen.util :as util]
            [knossos.history :as history]
            [clojure.core :as c]
            [clojure.tools.logging :refer [info]])
  (:import (java.util.concurrent.atomic AtomicBoolean)
           (java.util.concurrent.locks LockSupport)
           (java.util.concurrent CyclicBarrier)))

(defprotocol Generator
  (op [gen test process] "Yields an operation to apply."))

(extend-protocol Generator
  nil
  (op [this test process] nil)

  Object
  (op [this test process] this)

  ; Fns can generate ops by being called with test and process, or with no args
  clojure.lang.AFunction
  (op [f test process]
    (try
      (f test process)
      (catch clojure.lang.ArityException e
        (f)))))

(def ^:dynamic *threads*
  "The ordered collection of threads which will execute a particular generator.
  The special thread :nemesis is used for the nemesis; other threads are 0-n,
  where n is the test concurrency. Processes map to threads: process mod n is
  the thread ID.

  The set of threads is used where multiple parts of a test must synchronize.")

(defmacro with-threads
  "Binds *threads* for duration of body. Safety check: asserts threads are
  sorted."
  [threads & body]
  `(let [threads# ~threads]
     (assert (= threads# (history/sort-processes threads#)))
     (binding [*threads* threads#]
       ~@body)))

(defn process->thread
  "Given a process identifier, return the corresponding thread identifier."
  [test process]
  (if (integer? process)
    (mod process (:concurrency test))
    process))

(defn process->node
  "Given a test and a process identifier, returns the corresponding node this
  process is likely (clients aren't required to respect the node they're given)
  talking to, if process is an integer. Otherwise, nil."
  [test process]
  (let [thread (process->thread test process)]
    (when (integer? thread)
      (nth (:nodes test) (mod thread (count (:nodes test)))))))

(def void
  "A generator which terminates immediately"
  (reify Generator
    (op [gen test process])))

(defn sleep-til-nanos
  "High-resolution sleep; takes a time in nanos, relative to System/nanotime."
  [t]
  (while (< (+ (System/nanoTime) 10000) t)
    (LockSupport/parkNanos (- t (System/nanoTime)))))

(defn sleep-nanos
  "High-resolution sleep; takes a (possibly fractional) time in nanos."
  [dt]
  (sleep-til-nanos (+ dt (System/nanoTime))))

(defn delay-fn
  "Every operation from the underlying generator takes (f) seconds longer."
  [f gen]
  (reify Generator
    (op [_ test process]
      (Thread/sleep (* 1000 (f)))
      (op gen test process))))

(defn delay
  "Every operation from the underlying generator takes dt seconds to return."
  [dt gen]
  (delay-fn (constantly dt) gen))

(defn next-tick-nanos
  "Given a period `dt` (in nanos), beginning at some point in time `anchor`
  (also in nanos), finds the next tick after time `now`, such that the next
  tick is separate from anchor by an exact multiple of dt. If now is omitted,
  defaults to the current time."
  ([anchor dt]
   (next-tick-nanos anchor dt (util/linear-time-nanos)))
  ([anchor dt now]
   (+ now (- dt (mod (- now anchor) dt)))))

(defn delay-til
  "Operations are emitted as close as possible to multiples of dt seconds from
  some epoch. Where `delay` introduces a fixed delay between completion and
  invocation, delay-til attempts to schedule invocations as close as possible
  to the same time. This is useful for triggering race conditions.

  If precache? is true (the default), will pre-emptively request the next
  operation from the underlying generator, to eliminate jitter from that
  generator."
  ([dt gen]
   (delay-til dt true gen))
  ([dt precache? gen]
   (let [anchor (System/nanoTime)
         dt     (util/secs->nanos dt)]
     (if precache?
       (reify Generator
         (op [_ test process]
           (let [op (op gen test process)]
             (sleep-til-nanos (next-tick-nanos anchor dt))
             op)))
       (reify Generator
         (op [_ test process]
           (sleep-til-nanos (next-tick-nanos anchor dt))
           (op gen test process)))))))

(defn stagger
  "Introduces uniform random timing noise with a mean delay of dt seconds for
  every operation. Delays range from 0 to 2 * dt."
  [dt gen]
  (delay-fn (partial rand (* 2 dt)) gen))

(defn sleep
  "Takes dt seconds, and always produces a nil."
  [dt]
  (delay dt void))

(defn once
  "Wraps another generator, invoking it only once."
  [source]
  (let [emitted (AtomicBoolean. false)]
    (reify Generator
      (op [gen test process]
        (when-not (.get emitted)
          (when-not (.getAndSet emitted true)
            (op source test process)))))))

(defn log*
  "Logs a message every time invoked, and yields nil."
  [msg]
  (reify Generator
    (op [gen test process]
      (info msg)
      nil)))

(defn log
  "Logs a message only once, and yields nil."
  [msg]
  (once (log* msg)))

(defn each-
  "Takes a function that yields a generator. Invokes that function to
  create a new generator for each distinct process."
  [gen-fn]
  (let [processes (atom {})]
    (reify Generator
      (op [this test process]
        (if-let [gen (get @processes process)]
          (op gen test process)
          (do
            (swap! processes (fn [processes]
                               (if (contains? processes process)
                                 processes
                                 (assoc processes process (gen-fn)))))
            (recur test process)))))))

(defmacro each
  "Takes an expression evaluating to a generator. Captures that expression as a
  function, and constructs a generator that invokes that expression once for
  each process, as new processes arrive, such that each process sees an
  independent copy of the underlying generator."
  [gen-expr]
  `(each- (fn [] ~gen-expr)))

(defn seq
  "Given a sequence of generators, emits one operation from the first, then one
  from the second, then one from the third, etc. If a generator yields nil,
  immediately moves to the next. Yields nil once coll is exhausted."
  [coll]
  (let [elements (atom (cons nil coll))]
    (reify Generator
      (op [this test process]
        (when-let [gen (first (swap! elements next))]
          (if-let [op (op gen test process)]
            op
            (recur test process)))))))

(defn start-stop
  "A generator which emits a start after a t1 second delay, and then a stop
  after a t2 second delay."
  [t1 t2]
  (seq (cycle [(sleep t1)
               {:type :info :f :start}
               (sleep t2)
               {:type :info :f :stop}])))

(defn mix
  "A random mixture of operations. Takes a collection of generators and chooses
  between them uniformly."
  [gens]
  (let [gens (vec gens)]
    (reify Generator
      (op [_ test process]
        (op (rand-nth gens) test process)))))

(def cas
  "Random cas/read ops for a compare-and-set register over a small field of
  integers."
  (reify Generator
    (op [generator test process]
      (condp < (rand)
        0.66 {:type  :invoke
              :f     :read}
        0.33 {:type  :invoke
              :f     :write
              :value (rand-int 5)}
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

(defn filter
  "Takes a generator and yields a generator which emits only operations
  satisfying `(f op)`."
  [f gen]
  (reify Generator
    (op [_ test process]
      (loop []
        (when-let [op' (op gen test process)]
          (if (f op')
            op'
            (recur)))))))

(defn on
  [f source]
  "Forwards operations to source generator iff (f thread) is true. Rebinds
  *threads* appropriately."
  (reify Generator
    (op [gen test process]
      (when (f (process->thread test process))
        (binding [*threads* (c/filter f *threads*)]
          (op source test process))))))

(defn reserve
  "Takes a series of count, generator pairs, and a final default generator.

      (reserve 5 write 10 cas read)

  The first 5 threads will call the `write` generator, the next 10 will emit
  CAS operations, and the remaining threads will perform reads. This is
  particularly useful when you want to ensure that two classes of operations
  have a chance to proceed concurrently--for instance, if writes begin
  blocking, you might like reads to proceed concurrently without every thread
  getting tied up in a write.

  Rebinds *threads* appropriately. Assumes that every invocation of this
  generator arrives with the same binding for *threads*."
  [& args]
  (let [gens (->> args
                   drop-last
                   (partition 2)
                   ; Construct [lower upper gen] tuples defining the range of
                   ; thread indices covering a given generator, lower
                   ; inclusive, upper exclusive.
                   (reduce (fn [[n gens] [thread-count gen]]
                             (let [n' (+ n thread-count)]
                               [n' (conj gens [n n' gen])]))
                           [0 []])
                   second)
        default (last args)]
    (assert default)
    (reify Generator
      (op [_ test process]
        (let [threads (vec *threads*)
              thread  (process->thread test process)
              ; If our thread is smaller than some upper bound in gens, we've
              ; found our generator, since both *threads* and gens are ordered.
              [lower upper gen] (or (some (fn [[lower upper gen :as tuple]]
                                            (and (< thread (nth threads upper))
                                                 tuple))
                                          gens)
                                    ; Default fallback
                                    [(second (peek gens))
                                     (count threads)
                                     default])]
          (with-threads (subvec threads lower upper)
            (op gen test process)))))))

(defn concat
  "Takes n generators and yields the first non-nil operation from any, in
  order."
  [& sources]
  (reify Generator
    (op [gen test process]
      (loop [[source & sources] sources]
        (when source
          (if-let [op (op source test process)]
            op
            (recur sources)))))))

(defn nemesis
  "Combines a generator of normal operations and a generator for nemesis
  operations into one. When the process requesting an operation is :nemesis,
  routes to the nemesis generator; otherwise to the normal generator."
  ([nemesis-gen]
   (on #{:nemesis} nemesis-gen))
  ([nemesis-gen client-gen]
   (concat (on #{:nemesis} nemesis-gen)
           (on (complement #{:nemesis}) client-gen))))

(defn clients
  "Executes generator only on clients."
  ([client-gen]
   (on (complement #{:nemesis}) client-gen)))

(defn await
  "Blocks until the given fn returns, then allows [gen] to proceed. If no gen
  is given, yields nil. Only invokes fn once."
  ([f] (await f nil))
  ([f gen]
   (let [state (atom :waiting)]
     (reify Generator
       (op [_ test process]
         (when (= @state :waiting)
           (locking state
             (when (= @state :waiting)
               (f)
               (reset! state :ready))))
         (op gen test process))))))

(defn synchronize
  "Blocks until all nodes are blocked awaiting operations from this generator,
  then allows them to proceed. Only synchronizes a single time; subsequent
  operations on this generator proceed freely."
  [gen]
  (let [state (atom :fresh)]
    (reify Generator
      (op [_ test process]
        (when (not= :clear @state)
          ; Ensure a barrier exists
          (compare-and-set! state :fresh
                            (CyclicBarrier. (count *threads*)
                                            (partial reset! state :clear)))

          ; Block on barrier
          (.await ^CyclicBarrier @state))
        (op gen test process)))))

(defn phases
  "Like concat, but requires that all threads finish the first generator before
  moving to the second, and so on."
  [& generators]
  (apply concat (map synchronize generators)))

(defn then
  "Generator B, synchronize, then generator A. Why is this backwards? Because
  it reads better in ->> composition."
  [a b]
  (concat b (synchronize a)))

(defn singlethreaded
  "Obtaining an operation from the underlying generator requires an exclusive
  lock."
  [gen]
  (reify Generator
    (op [this test process]
      (locking this
        (op gen test process)))))

(defn barrier
  "When the given generator completes, synchronizes, then yields nil."
  [gen]
  (->> gen (then void)))
