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
            [clojure.walk :as walk]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [warn info]]
            [slingshot.slingshot :refer [throw+]]
            [tea-time.core :as tt])
  (:import (java.util.concurrent.atomic AtomicBoolean)
           (java.util.concurrent.locks LockSupport)
           (java.util.concurrent BrokenBarrierException
                                 CyclicBarrier)))

(defprotocol Generator
  (op [gen test process] "Yields an operation to apply."))

(defn op-and-validate
  "Wraps `op` to ensure we produce a valid operation for our
  worker. Re-throws any exception we catch from the generator."
  [gen test process]
  (let [op (op gen test process)]
    (when-not (or (nil? op) (map? op))
      (throw+ {:type :invalid-op
               :gen  gen
               :op   op}))
    op))

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

  The set of threads is used where multiple parts of a test must synchronize."
  nil)

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

;; Define lots of generators! ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro defgenerator
  "Like deftype, but the fields spec is followed by a vector of expressions
  which are used to print the datatype, and the Generator protocol is implicit.
  For instance:

      (defgenerator Delay [dt]
        [(str dt \" seconds\")] ; For pretty-printing
        (op [this test process] ; Function body
          ...))

  Inside the print-forms vector, every occurrence of a field name is replaced
  by (.some-field a-generator), so don't get fancy with let bindings or
  anything."
  [name fields print-forms & protocols-and-functions]
  (let [field-set (set fields)
        this-sym  (gensym "this")
        w-sym     (gensym "w")]
    `(do ; Standard deftype, just insert the generator and drop the print forms
         (deftype ~name ~fields Generator ~@protocols-and-functions)

         ; For prn/str, we'll generate a defmethod
         (defmethod print-method ~name
           [~this-sym ^java.io.writer ~w-sym]
           (.write ~w-sym
                   ~(str "(gen/" (.toLowerCase (clojure.core/name name))))
           ~@(mapcat (fn [field]
                       ; Rewrite field expression, changing field names to
                       ; instance variable getters
                       (let [field (walk/prewalk
                                     (fn [form]
                                       (if (field-set form)
                                         ; This was a field
                                         (list (symbol (str "." form))
                                               this-sym)
                                         ; Something else
                                         form))
                                     field)]
                         ; Spaces between fields
                         [`(.write ~w-sym " ")
                          `(print-method ~field ~w-sym)]))
                 print-forms)
           (.write ~w-sym ")")))))

(defgenerator GVoid [] []
  (op [gen test process]))

(def void
  "A generator which terminates immediately"
  (GVoid.))

(defgenerator FMap [f g]
  [f g]
  (op [gen test process]
      (update (op g test process) :f f)))

(defn f-map
  "Takes a function `f-map` converting op functions (:f op) to other functions,
  and a generator `g`. Returns a generator like `g`, but where fs are replaced
  according to `f-map`. Useful for composing generators together for use with a
  composed nemesis."
  [f-map g]
  (FMap. f-map g))

(defn sleep-til-nanos
  "High-resolution sleep; takes a time in nanos, relative to System/nanotime."
  [t]
  (while (< (+ (System/nanoTime) 10000) t)
    (LockSupport/parkNanos (- t (System/nanoTime)))))

(defn sleep-nanos
  "High-resolution sleep; takes a (possibly fractional) time in nanos."
  [dt]
  (sleep-til-nanos (+ dt (System/nanoTime))))

(defgenerator DelayFn [f gen]
  [f gen]
  (op [_ test process]
      (try
        (Thread/sleep (* 1000 (f)))
        (catch InterruptedException e
          nil))
      (op gen test process)))

(defn delay-fn
  "Every operation from the underlying generator takes (f) seconds longer."
  [f gen]
  (DelayFn. f gen))

(defn delay
  "Every operation from the underlying generator takes dt seconds to return."
  [dt gen]
  (assert (pos? dt))
  (delay-fn (constantly dt) gen))

(defn sleep
  "Takes dt seconds, and always produces a nil."
  [dt]
  (delay dt void))

(defn stagger
  "Introduces uniform random timing noise with a mean delay of dt seconds for
  every operation. Delays range from 0 to 2 * dt."
  [dt gen]
  (assert (pos? dt))
  (delay-fn (partial rand (* 2 dt)) gen))

(defn next-tick-nanos
  "Given a period `dt` (in nanos), beginning at some point in time `anchor`
  (also in nanos), finds the next tick after time `now`, such that the next
  tick is separate from anchor by an exact multiple of dt. If now is omitted,
  defaults to the current time."
  ([anchor dt]
   (next-tick-nanos anchor dt (util/linear-time-nanos)))
  ([anchor dt now]
   (+ now (- dt (mod (- now anchor) dt)))))

(defgenerator DelayTil
  [dt precache? anchor gen]
  [(util/nanos->secs dt) precache? gen]
  (op [_ test process]
     (if precache?
       (let [op (op gen test process)]
         (sleep-til-nanos (next-tick-nanos anchor dt))
         op)
       (do (sleep-til-nanos (next-tick-nanos anchor dt))
           (op gen test process)))))

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
     (DelayTil. dt precache? anchor gen))))

(defgenerator Once [source emitted]
  [(if (.get emitted) :done :pending) source]
  (op [gen test process]
      (when-not (.get emitted)
        (when-not (.getAndSet emitted true)
          (op source test process)))))

(defn once
  "Wraps another generator, invoking it only once."
  [source]
  (Once. source (AtomicBoolean. false)))

(defgenerator Derefer [dgen]
  [dgen]
  (op [this test process]
      (let [gen @dgen]
        (op gen test process))))

(defn derefer
  "Sometimes you need to build a generator not *now*, but *later*; e.g. because
  it depends on state that won't be available until the generator is actually
  invoked. Wrap a derefable returning a generator in this, and it'll be
  deref'ed every time an op is requested. For instance:

      (derefer (delay (gen/once {:type :drain-key, :value @key})))

  Looks up the key to drain only once an operation is requested."
  [dgen]
  (Derefer. dgen))

(defgenerator Log [msg]
  [msg]
  (op [gen test process]
      (info msg)
      nil))

(defn log*
  "Logs a message every time invoked, and yields nil."
  [msg]
  (Log. msg))

(defn log
  "Logs a message only once, and yields nil."
  [msg]
  (once (log* msg)))

(defgenerator Each [gen-fn processes]
  ; Sorry, this is a bit of a copout; I don't know how to render this *at all*.
  [gen-fn]
  (op [this test process]
    (if-let [gen (get @processes process)]
      (op gen test process)
      (do
        (swap! processes (fn [processes]
                           (if (contains? processes process)
                             processes
                             (assoc processes process (gen-fn)))))
        (recur test process)))))

(defn each-
  "Takes a function that yields a generator. Invokes that function to
  create a new generator for each distinct process."
  [gen-fn]
  (Each. gen-fn (atom {})))

(defmacro each
  "Takes an expression evaluating to a generator. Captures that expression as a
  function, and constructs a generator that invokes that expression once for
  each process, as new processes arrive, such that each process sees an
  independent copy of the underlying generator."
  [gen-expr]
  `(each- (fn [] ~gen-expr)))

(defgenerator Seq [elements]
  ; Don't try to render an infinite sequence
  [(let [es (take 8 (next @elements))]
     (if (<= (count es) 8)
       es
       (c/concat es ['...])))]
  (op [this test process]
    (when-let [gen (first (swap! elements next))]
      (if-let [op (op gen test process)]
        op
        (recur test process)))))

(defn seq
  "Given a sequence of generators, emits one operation from the first, then one
  from the second, then one from the third, etc. If a generator yields nil,
  immediately moves to the next. Yields nil once coll is exhausted."
  [coll]
  (Seq. (atom (cons nil coll))))

(defn start-stop
  "A generator which emits a start after a t1 second delay, and then a stop
  after a t2 second delay."
  [t1 t2]
  (seq (cycle [(sleep t1)
               {:type :info :f :start}
               (sleep t2)
               {:type :info :f :stop}])))

(defgenerator Mix [gens]
  [gens]
  (op [_ test process]
      (op (rand-nth gens) test process)))

(defn mix
  "A random mixture of operations. Takes a collection of generators and chooses
  between them uniformly. If the collection is empty, generator returns nil."
  [gens]
  (let [gens (vec gens)]
    (if (empty? gens)
      void
      (Mix. (vec gens)))))

;; Test-specific generators -- should probably move these to jepsen.tests/*
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

(defgenerator Limit [gen remaining]
  [(dec @remaining) gen]
  (op [_ test process]
      (when (pos? (swap! remaining dec))
        (op gen test process))))

(defn limit
  "Takes a generator and returns a generator which only produces n operations."
  [n gen]
  (Limit. gen (atom (inc n))))


; T I M E   L I M I T S
;
; Our general approach here is to schedule a task which will interrupt as
; at the deadline. To figure out who to interrupt, we keep a set of
; threads currently engaged in this time-limit; the deadline task
; interrupts every thread in that set.
;
; To avoid a race condition where the deadline sees a thread that is
; currently in the time-limit, that thread returns from the time-limit
; and goes on to do something else, and we interrupt, it, we have a
; *lock* on threads which prevents the deadline from killing threads once
; they're on the way out.
;
; Now, the question is: what do we *do* with that interrupt? Our goal is
; to return nil when we hit the time limit, but we *also* need to play
; nicely with other potential causes of interrupts--for instance, nested
; time-limits either enclosing us, or which we enclose.
;
; You might *think* that we could return nil on any interrupt, but this
; might be incorrect. For instance, if we wrap a sequence of time-limited
; operations, and the first one times out, the *second* one should get a
; chance to execute--it should return nil, and we should never observe
; its interruption.
;
; If an *enclosing* time limit fires and interrupts us, we have an
; inverse problem: we might be surrounded by a generator that sees us run
; out of operations and moves on to a new generator instead. Therefore we
; *must* propagate interruptions triggered by our enclosing time limits.
;
; Essentially, we need a *side channel* to tell us: did *we* interrupt
; ourselves, or did someone else? To do this, we store a local promise,
; `interrupted?`, which the deadline task flips to `true` if it's going
; to interrupt the thread. If we've interrupted ourselves, then we return
; nil. If not, it might be some enclosing interrupt, which needs to
; handle it instead; in that case, we rethrow.
;
; "Ah, but Kyle!? You exclaim, "What if two time limits fire
; concurrently, and we are interrupted *while performing* this
; bookkeeping!"
;
; This is a serious problem. You can be interrupted during `finally`. We
; can't eliminate all concurrent interrupts, but we *can* eliminate our own.
; We'll set up a *global* lock on the time-limit fn, and use the thread set to
; make sure no other time-limit can interrupt a worker while the others are
; triggering.
(declare time-limit)
(defgenerator TimeLimit [dt                 ; Time interval
                         source             ; Underlying generator
                         deadline           ; End time in micros
                         threads            ; Atom: set of active threads
                         interrupted?]      ; Atom: are we being interrupted?
  [dt source]

  (op [_ test process]
      (try
        ; Initialize start time on first run
        (when (nil? @deadline)
          (let [d' (+ (tt/linear-time-micros)
                      (tt/seconds->micros dt))]
            (when (compare-and-set! deadline nil d')
              ; We won the CAS to initialize our deadline. Schedule task to
              ; interrupt active threads later.
              (tt/at-linear-micros! d' (fn []
                                         (locking time-limit
                                           ; Mark ourself as interrupting
                                           (reset! interrupted? true)

                                           ; Interrupt those threads
                                           (locking threads
                                             (doseq [t @threads]
                                               (.interrupt t)))

                                           ; Wait for interrupt processing to
                                           ; complete
                                           (while (pos? (count @threads))
                                             (Thread/sleep 100))

                                           ; Clean up so future interrupts
                                           ; aren't trapped in our handler
                                           (reset! interrupted? false)))))))

        (when (<= (tt/linear-time-micros) @deadline)
          ; We haven't passed the deadline yet. Register this thread in the
          ; thread set
          (try
            (swap! threads conj (Thread/currentThread))
            ; Grab an operation from the underlying generator
            (op source test process)
            (finally
              (locking threads
                (swap! threads disj (Thread/currentThread))))))

        (catch InterruptedException sea-lion
          ; Okay. SOMEONE interrupted us. We're definitely not executing any
          ; more. We could have been interrupted FROM the finally block, so
          ; we need to remove ourselves from the executing set again...
          (locking threads
            (swap! threads disj (Thread/currentThread)))
          (if @interrupted?
            nil
            (throw sea-lion)))

        ; Ugh this is SUCH a special case thing but BrokenBarrier is also a
        ; sort of interrupted exception???
        (catch BrokenBarrierException sea-lion
          (locking threads
            (swap! threads disj (Thread/currentThread)))
          (if @interrupted?
            nil
            (throw sea-lion))))))

(defn time-limit
  "Yields operations from the underlying generator until dt seconds have
  elapsed."
  [dt source]
  (TimeLimit. dt source (atom nil) (atom #{}) (atom false)))

(defgenerator Filter [f gen]
  [f gen]
  (op [_ test process]
      (loop []
        (when-let [op' (op gen test process)]
          (if (f op')
            op'
            (recur))))))

(defn filter
  "Takes a generator and yields a generator which emits only operations
  satisfying `(f op)`."
  [f gen]
  (Filter. f gen))

(defgenerator On [f source]
  [f source]
  (op [gen test process]
      (when (f (process->thread test process))
        (binding [*threads* (c/filter f *threads*)]
          (op source test process)))))

(defn on
  [f source]
  "Forwards operations to source generator iff (f thread) is true. Rebinds
  *threads* appropriately."
  (On. f source))

(defgenerator Reserve [gens default]
  [gens default]
  (op [_ test process]
      (let [threads (vec *threads*)
            thread  (process->thread test process)
            ;_ (info threads thread)
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
          (op gen test process)))))

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
    (Reserve. gens default)))

; processes is a map of process to the index of the source they're currently on.
(defgenerator Concat [sources processes]
  [sources]
  (op [gen test process]
    (let [i (get @processes process 0)]
      (when (< i (count sources))
        (let [source (nth sources i)]
          (if-let [op (op source test process)]
            op
            (do (swap! processes (fn [processes]
                                   (if (= i (get processes process 0))
                                     ; Good, nobody else has changed us
                                     (assoc processes process (inc i))
                                     ; Someone else beat us
                                     processes)))
                (recur test process))))))))

(defn concat
  "Takes n generators and yields the first non-nil operation from any, in
  order."
  [& sources]
  (Concat. (vec sources) (atom {})))

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

(defgenerator Await [f gen state]
  [f @state gen]
  (op [_ test process]
      (when (= @state :waiting)
        (locking state
          (when (= @state :waiting)
            (f)
            (reset! state :ready))))
      (op gen test process)))

(defn await
  "Blocks until the given fn returns, then allows [gen] to proceed. If no gen
  is given, yields nil. Only invokes fn once."
  ([f] (await f nil))
  ([f gen]
   (Await. f gen (atom :waiting))))

(defgenerator Synchronize [gen state]
  [@state gen]
  (op [_ test process]
    (when (not= :clear @state)
      ; Ensure a barrier exists
      (compare-and-set! state :fresh
                        (CyclicBarrier. (count *threads*)
                                        (partial reset! state :clear)))

      ; Block on barrier
      (.await ^CyclicBarrier @state))
    (op gen test process)))

(defn synchronize
  "Blocks until all nodes are blocked awaiting operations from this generator,
  then allows them to proceed. Only synchronizes a single time; subsequent
  operations on this generator proceed freely."
  [gen]
  (Synchronize. gen (atom :fresh)))

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
  lock. TODO: is this actually used by anyone? Feels kind of silly given
  everythng else is concurrency-safe."
  [gen]
  (reify Generator
    (op [this test process]
      (locking this
        (op gen test process)))))

(defn barrier
  "When the given generator completes, synchronizes, then yields nil."
  [gen]
  (->> gen (then void)))
