(ns jepsen.generator.context
  "Generators work with an immutable *context* that tells them what time it is,
  what processes are available, what process is executing which thread and vice
  versa, and so on. We need an efficient, high-performance data structure to
  track this information. This namespace provides that data structure, and
  functions to alter it.

  Contexts are intended not only for managing generator-relevant state about
  active threads and so on; they also can store arbitrary contextual
  information for generators. For instance, generators may thread state between
  invocations or layers of the generator stack. To do this, contexts *also*
  behave like Clojure maps. They have a single special key, :time; all other
  keys are available for your use."
  (:require [clojure.core.protocols :refer [Datafiable]]
            [clojure [datafy :refer [datafy]]]
            [dom-top.core :refer [loopr]]
            [jepsen.generator.translation-table :as tt]
            [potemkin :refer [def-map-type definterface+]])
  (:import (io.lacuna.bifurcan IEntry
                               ISet
                               IMap
                               Map
                               Set)
           (java.util BitSet)
           (jepsen.generator.translation_table TranslationTable)))

;; Just for debugging
(extend-protocol Datafiable
  BitSet
  (datafy [this]
    (loop [i 0
           s (sorted-set)]
     (let [i (.nextSetBit this i)]
       (if (= i -1)
         s
         (recur (inc i)
                (conj s i))))))

  IMap
  (datafy [this]
    (persistent!
      (reduce (fn [m ^IEntry pair]
                (assoc! m (.key pair) (.value pair)))
              (transient {})
              this))))

;; Contexts

(definterface+ IContext
  (^ISet all-threads [ctx]
               "Given a context, returns a Bifurcan ISet of all threads in it.")

  (all-thread-count [ctx]
                    "How many threads are in the given context, total?")

  (free-thread-count [ctx]
                     "How many threads are free in the given context?")

  (all-processes [ctx]
                 "Given a context, returns a Bifurcan ISet of all processes
                 currently belonging to some thread.")

  (process->thread [ctx process]
                   "Given a process, looks up which thread is executing it.")

  (thread->process [ctx thread]
                   "Given a thread, looks up which process it's executing.")

  (thread-free? [ctx thread]
                "Is the given thread free?")

  (^ISet free-threads [ctx]
                "Given a context, returns a Bifurcan ISet of threads which are
                not actively processing an invocation.")

  (free-processes [ctx]
                  "Given a context, returns a collection of processes which are
                  not actively processing an invocation.")

  (some-free-process [ctx]
                     "Given a context, returns a random free process, or nil if
                     all are busy.")

  (busy-thread [this time thread]
                "Returns context with the given time, and the given thread no
                longer free.")

  (free-thread [this time thread]
               "Returns context with the given time, and the given thread
               free.")

  (with-next-process [ctx thread]
    "Replaces a thread's process with a new one."))

(def-map-type Context
  [; Our time
   ^long time
   ; The next thread index we'd like to hand out
   ^int next-thread-index
   ; A translation table for thread names. May include threads not in this
   ; context.
   ^TranslationTable translation-table
   ; A bitset of thread indices which are active in this context
   ^BitSet all-threads
   ; A bitset of thread indices which are not busy evaluating anything
   ^BitSet free-threads
   ; An array which maps thread indices to the process they're currently
   ; executing. May include threads not in this context.
   ^objects thread-index->process
   ; A map of processes to the thread currently executing them. May include
   ; threads not in this context.
   ^IMap process->thread
   ; A Clojure map used for any custom fields users assign.
   ext-map]

  ; Our map implementation basically proxies to ext-map, except for :time
  (get [_ k default]
       (condp identical? k
         :time    time
         ; We can remove these later, but it'll be nice to warn users that
         ; these are gone.
         :workers (throw (UnsupportedOperationException. "Removed; use jepsen.generator.context/all-threads et al"))
         :free-threads (throw (UnsupportedOperationException. "Removed; use jepsen.generator.context/free-threads et al"))
         (get ext-map k default)))

  (assoc [_ k v]
       (condp identical? k
         :time    (Context. v next-thread-index
                            translation-table all-threads free-threads
                            thread-index->process process->thread ext-map)
         ; We can remove these later, but it'll be nice to warn users that
         ; these are gone.
         :workers (throw (UnsupportedOperationException. "Removed; use jepsen.generator.context/all-threads et al"))
         :free-threads (throw (UnsupportedOperationException. "Removed; use jepsen.generator.context/free-threads"))
         (Context. time next-thread-index translation-table all-threads
                   free-threads thread-index->process process->thread
                   (assoc ext-map k v))))

  (dissoc [_ k]
          (condp identical? k
            :time (throw (IllegalArgumentException. "Can't dissoc :time from a context!"))
            (Context. time next-thread-index translation-table all-threads
                      free-threads thread-index->process process->thread
                      (dissoc ext-map k))))

  (keys [_]
        (cons :time (keys ext-map)))

  (meta [_]
        (meta ext-map))

  (with-meta [_ mta]
             (Context. time next-thread-index translation-table all-threads
                       free-threads thread-index->process process->thread
                       (with-meta ext-map mta)))

  ; For debugging
  Datafiable
  (datafy [this]
    {:time                  time
     :next-thread-index     next-thread-index
     :translation-table     (datafy translation-table)
     :all-threads           (datafy all-threads)
     :free-threads          (datafy free-threads)
     :thread-index->process (vec thread-index->process)
     :process->thread       (datafy process->thread)
     :ext-map               (datafy ext-map)})

  ; Oh yeah, we're supposed to be a context too
  IContext
  (all-threads [this]
               (tt/indices->names translation-table all-threads))

  (all-thread-count [this]
                    (.cardinality all-threads))

  (free-thread-count [this]
                     (.cardinality free-threads))

  (all-processes [this]
    (mapv (partial thread->process this)
          (.all-threads this)))

  (process->thread [this process]
    (.get process->thread process nil))

  (thread->process [this thread]
                   (let [i (tt/name->index translation-table thread)]
                     (aget thread-index->process i)))

  (free-threads [this]
                (tt/indices->names translation-table free-threads))

  (free-processes [this]
    (mapv (partial thread->process this)
          (.free-threads this)))

  (thread-free? [this thread]
                (let [i (tt/name->index translation-table thread)]
                  (.get free-threads i)))

  (some-free-process [this]
    (let [i (.nextSetBit free-threads next-thread-index)]
      (cond (<= 0 i)
            ; Found something!
            (aget thread-index->process i)

            ; Found nothing, and we checked the whole set
            (= 0 next-thread-index)
            nil

            ; Loop around and check from the start
            true
            (let [i (.nextSetBit free-threads 0)]
              (if (= -1 i)
                ; Definitely empty!
                nil
                (aget thread-index->process i))))))

  (free-thread [this time thread]
    (let [i (tt/name->index translation-table thread)]
      (Context. time next-thread-index translation-table all-threads
                (doto ^BitSet (.clone free-threads)
                  (.set i))
                thread-index->process process->thread ext-map)))

  (busy-thread [this time thread]
    (let [i (tt/name->index translation-table thread)]
      ; When we consume a thread, we bump the next thread index. This means we
      ; rotate evenly through threads instead of giving a single thread all the
      ; ops.
      (Context. time
                (mod (inc next-thread-index)
                     (tt/thread-count translation-table))
                translation-table all-threads
                (doto ^BitSet (.clone free-threads)
                  (.clear i))
                thread-index->process process->thread ext-map)))

  (with-next-process [this thread]
    (let [process  (thread->process this thread)
          process' (if (integer? process)
                     (+ (.int-thread-count translation-table)
                        process)
                     process)
          i        (tt/name->index translation-table thread)
          n        (alength thread-index->process)
          thread-index->process' (aclone thread-index->process)]
      (aset thread-index->process' i process')
      (Context. time next-thread-index translation-table all-threads
                free-threads
                thread-index->process'
                (.. process->thread
                    (remove process)
                    (put process' thread))
                ext-map))))

(defn context
  "Constructs a fresh Context for a test. Its initial time is 0. Its threads
  are the integers from 0 to (:concurrency test), plus a :nemesis). Every
  thread is free. Each initially runs itself as a process."
  [test]
  (let [named-threads      [:nemesis]
        translation-table  (tt/translation-table (:concurrency test)
                                                 named-threads)
        thread-count       (tt/thread-count translation-table)
        thread-names       (tt/all-names translation-table)
        ; Initially all threads are in the context and free
        all-threads-bitset (doto (BitSet. thread-count)
                                 (.set 0 thread-count))
        ; Everyone initially executes themselves
        thread-index->process (object-array thread-names)
        process->thread (loopr [^IMap m (.linear Map/EMPTY)]
                               [thread thread-names]
                               (recur (.put m thread thread))
                               (.forked m))]
    (Context.
      0 ; Time
      0 ; Next thread
      translation-table
      all-threads-bitset
      all-threads-bitset
      thread-index->process
      process->thread
      ; Ext map
      {})))

;; Restricting contexts to specific threads

(defrecord AllBut [element]
  clojure.lang.IFn
  (invoke [_ x]
    (if (= element x)
      nil
      x)))

(defn all-but
  "One thing we do often, and which is expensive, is stripping out the nemesis
  from the set of active threads using (complement #{:nemesis}). This type
  encapsulates that notion of \"all but x\", and allows us to specialize some
  expensive functions for speed."
  [x]
  (AllBut. x))

(defn intersect-bitsets
  "Intersects one bitset with another, immutably."
  [^BitSet a ^BitSet b]
  (doto ^BitSet (.clone a)
    (.and b)))

(defn make-thread-filter
  "We often want to restrict a context to a specific subset of threads matching
  some predicate. We want to do this a *lot*. To make this fast, we can
  pre-compute a function which does this restriction more efficiently than
  doing it at runtime.

  Call this with a context and a predicate, and it'll construct a function
  which restricts any version of that context (e.g. one with the same threads,
  but maybe a different time or busy state) to just threads matching the given
  predicate.

  Don't have a context handy? Pass this just a predicate, and it'll construct a
  filter which lazily compiles itself on first invocation, and is fast
  thereafter."
  ; Lazy
  ([pred]
   (let [thread-filter (promise)]
     (fn lazy-filter [ctx]
       (if (realized? thread-filter)
         (@thread-filter ctx)
         (let [tf (make-thread-filter pred ctx)]
           (deliver thread-filter tf)
           (tf ctx))))))
  ; Explicit precomputation
  ([pred ^Context ctx]
   ; Compute a bitset of thread indices in advance
   (let [tt             (.translation-table ctx)
         ^BitSet bitset (.clone ^BitSet (.-all-threads ctx))]
     ; Compute a subset of our thread set which matches the given predicate.
     (loop [i 0]
       (let [i (.nextSetBit bitset i)]
         (when-not (= i -1)
           ; We've got indices to consider
           (when-not (pred (tt/index->name tt i))
             ; This isn't in the set; clear it
             (.clear bitset i))
           (recur (inc i)))))

     ; And here's a function that intersects the threads with that bitset.
     (fn by-bitset [^Context ctx]
       (Context. (.time ctx)
                 (.next-thread-index ctx)
                 (.translation-table ctx)
                 (intersect-bitsets bitset (.-all-threads ctx))
                 (intersect-bitsets bitset (.-free-threads ctx))
                 (.thread-index->process ctx)
                 (.-process->thread ctx)
                 (.ext-map ctx))))))
