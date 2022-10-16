(ns jepsen.generator.context
  "Generators work with an immutable *context* that tells them what time it is,
  what processes are available, what process is executing which thread and vice
  versa, and so on. We need an efficient, high-performance data structure to
  track this information. This namespace provides that data structure, and
  functions to alter it."
  (:require [dom-top.core :refer [loopr]]
            [potemkin :refer [definterface+]])
  (:import (io.lacuna.bifurcan ISet
                               IMap
                               Map
                               Set)))

(definterface+ IContext
  (^ISet all-threads [ctx]
               "Given a context, returns a Bifurcan ISet of all threads in it.")

  (all-processes [ctx]
                 "Given a context, returns a collection of all processes
                 currently belonging to some thread.")

  (process->thread [ctx process]
                   "Given a process, looks up which thread is executing it.")

  (thread->process [ctx thread]
                   "Given a thread, looks up which process it's executing.")

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

(defn next-process
  "When a process being executed by a thread crashes, this function returns the
  next process for that thread. You should probably only use this for the
  global context (rather than one restricted to a specific subset of threads),
  or it might compute colliding values for new processes."
  [context thread]
  (if (number? thread)
    (+ (thread->process context thread)
       (count (filter number? (all-processes context))))
    thread))

(defn all-thread-count
  "How many threads does a context have in total?"
  [context]
  (.size ^ISet (all-threads context)))

(defn free-thread-count
  "How many threads are free on the given context?"
  [context]
  (.size ^ISet (free-threads context)))

(defrecord Context [^long time
                    ; A set of all threads active in this context.
                    ^ISet all-threads
                    ; A set of threads which are not busy evaluating anything.
                    ; May include threads *not* in this context.
                    ^ISet free-threads
                    ; A map of threads to the process they're currently
                    ; executing. May include threads *not* in this context.
                    ^IMap thread->process
                    ; A map of processes to the thread executing them. May
                    ; include threads *not* in this context.
                    ^IMap process->thread]
  IContext
  (all-threads [this]
    all-threads)

  (all-processes [this]
    (mapv (fn [thread]
            (.get thread->process thread nil))
          all-threads))

  (process->thread [this process]
    (.get process->thread process nil))

  (thread->process [this thread]
    (.get thread->process thread nil))

  (free-threads [this]
    free-threads)

  (free-processes [this]
    (mapv (fn [thread]
            (.get thread->process thread nil))
          free-threads))

  (some-free-process [this]
    (let [n (.size free-threads)]
      (when-not (zero? n)
        (let [thread (.nth free-threads (rand-int n))]
          (.get thread->process thread nil)))))

  (free-thread [this time thread]
    (assoc this
           :time time
           :free-threads (.add free-threads thread)))

  (busy-thread [this time thread]
    (assoc this
           :time time
           :free-threads (.remove free-threads thread)))

  (with-next-process [this thread]
    (let [process  (.get thread->process thread nil)
          process' (next-process this thread)]
      (assoc this
             :thread->process (.put thread->process thread process')
             :process->thread (.. process->thread
                                  (remove process)
                                  (put process' thread))))))

(defn context
  "Constructs a fresh Context for a test. Its initial time is 0. Its threads
  are the integers from 0 to (:concurrency test), plus a :nemesis). Every
  thread is free. Each initially runs itself as a process."
  [test]
  (let [threads (->> (range (:concurrency test))
                     (cons :nemesis))
        threads (.forked (Set/from ^Iterable threads))
        ; Everyone initially executes themselves
        thread<->process (loopr [^IMap m (.linear Map/EMPTY)]
                                [thread threads]
                                (recur (.put m thread thread))
                                (.forked m))]
    (map->Context
      {:time            0
       :all-threads     threads
       :free-threads    threads
       :thread->process thread<->process
       :process->thread thread<->process})))

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

(defn on-threads-context-fn
  "A version of on-threads-context which takes an arbitrary filtering function.
  Runs in linear time."
  [^Context ctx f]
  ; Filter the all and free threads in one pass.
  (let [^ISet free-threads (.free-threads ctx)]
    (loopr [^ISet all-threads'  (.linear Set/EMPTY)
            ^ISet free-threads' (.linear Set/EMPTY)]
           [thread (all-threads ctx)]
           (if (f thread)
             (recur (.add all-threads' thread)
                    (if (.contains free-threads thread)
                      (.add free-threads' thread)
                      free-threads'))
             (recur all-threads' free-threads'))
           (assoc ctx
                  :all-threads  (.forked all-threads')
                  :free-threads (.forked free-threads')))))

(defn on-threads-context-set
  "A version of on-threads-context which is optimized for explicit sets. Runs
  in time proportional to the set, rather than the context size. This makes
  e.g. Reserve more efficient."
  [^Context ctx thread-set]
  (let [^ISet free-threads (.free-threads ctx)]
    (loopr [^ISet all-threads' (.linear Set/EMPTY)
            ^ISet free-threads' (.linear Set/EMPTY)]
           [thread thread-set]
           (recur (.add all-threads' thread)
                  (if (.contains free-threads thread)
                    (.add free-threads' thread)
                    free-threads'))
           (assoc ctx
                  :all-threads  (.forked all-threads')
                  :free-threads (.forked free-threads')))))

(defn on-threads-context-all-but
  "A version of on-threads-context specialized for AllBut, removing a single
  element--usually the nemesis."
  [^Context ctx ^AllBut all-but]
  (let [thread (.element all-but)]
    (assoc ctx
           :all-threads  (.remove ^ISet (.all-threads ctx) thread)
           :free-threads (.remove ^ISet (.free-threads ctx) thread))))

(defn on-threads-context
  "Restricts a context to a particular group of threads. Takes a set or a
  function which returns truthy values for threads to preserve, or an AllBut
  with a thread to remove. Returns a context with just those threads."
  [f ctx]
  (cond (set? f)             (on-threads-context-set      ctx f)
        (instance? AllBut f) (on-threads-context-all-but  ctx f)
        true                 (on-threads-context-fn       ctx f)))
