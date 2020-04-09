(ns jepsen.generator.pure
  "A Jepsen history is a list of operations--invocations and completions. A
  generator's job is to specify what invocations to perform, and when. In a
  sense, a generator *becomes* a history as Jepsen incrementally applies it to
  a real system.

  Naively, we might define a history as a fixed sequence of invocations to
  perform at certain times, but this is impossible: we have only a fixed set of
  threads, and they may not be free to perform our operations. A thread must be
  *free* in order to perform an operation.

  Time, too, is a dependency. When we schedule an operation to occur once per
  second, we mean that only once a certain time has passed can the next
  operation begin.

  There may also be dependencies between threads. Perhaps only after a nemesis
  has initiated a network partition does our client perform a particular read.
  We want the ability to hold until a certain operation has begun.

  Conceptually, then, a generator is a *graph* of events, some of which have
  not yet occurred. Some events are invocations: these are the operations the
  generator will provide to clients. Some events are completions: these are
  provided by clients to the generator. Other events are temporal: a certain
  time has passed.

  This graph has some invocations which are *ready* to perform. When we have a
  ready invocation, we apply the invocation using the client, obtain a
  completion, and apply the completion back to the graph, obtaining a new
  graph.

  ## Contexts

  A *context* is a map which provides extra information for generators. For
  instance, a generator might need to know the number of threads which will ask
  it for operations. It can get that number from the *context*. Users can add
  their own values to the context map, which allows two generators to share
  state. When one generator calls another, it can pass a modified version of
  the context, which allows us to write generators that, say, run two
  independent workloads, each with their own concurrency and thread mappings.

  The standard context mappings, which are provided by Jepsen when invoking the
  top-level generator, and can be expected by every generator, are:

      :time           The current Jepsen linear time, in nanoseconds
      :free-threads   A collection of idle threads which could perform work
      :workers        A map of thread identifiers to process identifiers

  ## Fetching an operation

  We use `(op gen test context)` to ask the generator for the next invocation
  that we can process.

  The operation can have three forms:

  - The generator may return `nil`, which means the generator is done, and
    there is nothing more to do. Once a generator does this, it must never
    return anything other than `nil`, even if the context changes.
  - The generator may return :pending, which means there might be more
    ops later, but it can't tell yet.
  - The generator may return an operation, in which case:
    - If its time is in the past, we can evaluate it now
    - If its time is in the future, we wait until either:
      - The time arrives
      - Circumstances change (e.g. we update the generator)

  But (op gen test context) returns more than just an operation; it also
  returns the *subsequent state* of the generator, if that operation were to be
  performed. The two are bundled into a tuple.

  (op gen test context) => [op gen']      ; known op
                           [:pending gen] ; unsure
                           nil            ; exhausted

  The analogous operations for sequences are (first) and (next); why do we
  couple them here? Why not use the update mechanism strictly to evolve state?
  Because the behavior in sequences is relatively simple: next always moves
  forward one item, whereas only *some* updates actually cause systems to move
  forward. Seqs always do the same thing in response to `next`, whereas
  generators may do different things depending on context. Moreover, Jepsen
  generators are often branched, rather than linearly wrapped, as sequences
  are, resulting in questions about *which branch* needs to be updated.

  When I tried to strictly separate implementations of (op) and (update), it
  resulted in every update call attempting to determine whether this particular
  generator did or did not emit the given invocation event. This is
  *remarkably* tricky to do well, and winds up relying on all kinds of
  non-local assumptions about the behavior of the generators you wrap, and
  those which wrap you.

  ## Updating a generator

  We still want the ability to respond to invocations and completions, e.g. by
  tracking that information in context variables. Therefore, in addition to
  (op) returning a new generator, we have a separate function, (update gen test
  context event), which allows generators to react to changing circumstances.

  - We invoke an operation (e.g. one that the generator just gave us)
  - We complete an operation

  Updates use a context with a specific relationship to the event:

  - The context :time is equal to the event :time
  - The free processes and worker maps reflect the state after the event has
    taken place; e.g. if the event is an invoke, the thread is listed as no
    longer free; if the event is a completion, the thread is listed as free.

  ## Default implementations

  Nil is a valid generator; it ignores updates and always yields nil for
  operations.

  IPersistentMaps are generators which ignore updates and return operations
  which look like the map itself, but with default values for time, process,
  and type provided based on the context. This means you can write a generator
  like

  {:f :write, :value 2}

  and it will generate ops like

  {:type :invoke, :process 3, :time 1234, :f :write, :value 2}

  Sequences are generators which assume the elements of the sequence are
  themselves generators. They ignore updates, and return all operations from
  the first generator in the sequence, then all operations from the second, and
  so on. They do not synchronize.

  Functions are generators which ignore updates and can take either test and
  context as arguments, or no args. Functions should be *mostly* pure, but some
  creative impurity is probably OK. For instance, returning randomized :values
  for maps is probably all right. I don't know the laws! What is this, Haskell?

  Functions can return two things:

  - nil: signifies that the function generator is exhausted.
  - a tuple of [op gen]: passed through directly; the gen replaces the fn
  - a map: the map is treated as a generator, which lets it fill in a process,
           time, etc.

  In the future, we might consider:

  - Returning any other generator: the function could be *replaced* by that
  generator, allowing us to compute generators lazily?
  "
  (:refer-clojure :exclude [await concat delay filter map repeat run! update])
  (:require [clojure.core :as c]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer [info warn error]]
            [jepsen [util :as util]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent ArrayBlockingQueue
                                 TimeUnit)))

(defprotocol Generator
  (update [gen test context event]
          "Updates the generator to reflect an event having taken place.")

  (op [gen test context]
      "Obtains the next operation from this generator."))

;; Helpers

(defn random-int-seq
  "Generates a reproducible sequence of random longs, given a random seed. If
  seed is not provided, taken from (rand-int))."
  ([] (random-int-seq (rand-int Integer/MAX_VALUE)))
  ([seed]
   (let [gen (java.util.Random. seed)]
     (repeatedly #(.nextLong gen)))))

(defn free-processes
  "Given a context, returns a collection of processes which are not actively
  processing invocations."
  [context]
  (c/map (:workers context) (:free-threads context)))

(defn some-free-process
  "Faster than (first (free-processes ctx)), because we don't map over the
  whole free-threads collection."
  [context]
  (get (:workers context) (first (:free-threads context))))

(defn all-processes
  "Given a context, returns all processes currently being executed by threads."
  [context]
  (vals (:workers context)))

(defn free-threads
  "Given a context, returns a collection of threads that are not actively
  processing invocations."
  [context]
  (:free-threads context))

(defn all-threads
  "Given a context, returns a collection of all threads."
  [context]
  (keys (:workers context)))

(defn process->thread
  "Takes a context and a process, and returns the thread which is executing
  that process."
  [context process]
  (->> (:workers context)
       (keep (fn [[t p]] (when (= process p) t)))
       first))

(defn next-process
  "When a process being executed by a thread crashes, this function returns the
  next process for a given thread. You should probably only use this with the
  *global* context, because it relies on the size of the `:workers` map."
  [context thread]
  (if (number? thread)
    (+ (get (:workers context) thread)
       (count (c/filter number? (all-processes context))))
    thread))

;; Interpreter


(defn worker
  "Creates communication channels and spawns a worker thread to evaluate the
  given worker. Takes a Queue which should emit completion operations, a
  function to evaluate invocations and produce completions, and a worker name.
  Returns a map with:

    :worker   The worker ID
    :future   The future evaluating the worker code
    :in       A Queue which delivers invocations to the worker"
  [^ArrayBlockingQueue out invoke! worker]
  (let [in (ArrayBlockingQueue. 1)]
    {:worker  worker
     :in      in
     :future  (future
                (util/with-thread-name (str "jepsen worker "
                                            (util/name+ worker))
                  (try
                    (loop []
                      (let [op (.take in)]
                        (case (:type op)
                          ; We're done here
                          :exit   nil

                          ; Ahhh
                          :sleep  (do (Thread/sleep (* 1000 (:value op)))
                                      (.put out op)
                                      (recur))

                          ; Log a message
                          :log    (do (.put out op)
                                      (recur))

                          ; Ask the invoke handler
                          (let [op' (invoke! op)]
                            (.put out op')
                            (recur)))))
                    (catch Throwable t
                      (error t "Jepsen worker crashed!")))))}))

(def ^Long/TYPE max-pending-interval
  "When the generator is :pending, this controls the maximum interval before
  we'll update the context and check the generator for an operation again.
  Measured in microseconds."
  1000)

(defn goes-in-history?
  "Should this operation be journaled to the history? We exclude :log and
  :sleep ops right now."
  [op]
  (condp identical? (:type op)
    :sleep false
    :log   false
    true))

(defn run!
  "Takes a function `invoke`, which evaluates an invocation and returns a
  completion, a test, an initial context `ctx`, and a generator `gen`. Spawns a
  thread for each worker, and hands those workers operations from gen; each
  thread applies the operation using `invoke`. Invocations and completions are
  journaled to a history, which is returned at the end of `run`."
  [invoke! test ctx gen]
  (let [worker-ids (all-threads ctx)
        completions (ArrayBlockingQueue. (count worker-ids))
        workers     (mapv (partial worker completions invoke!) worker-ids)
        invocations (into {} (c/map (juxt :worker :in) workers))]
    (loop [ctx            ctx
           gen            gen
           outstanding    0     ; Number of in-flight ops
           ; How long to poll on the completion queue, in micros.
           poll-timeout   0
           history        (transient [])]
      ; First, can we complete an operation? We want to get to these first
      ; because they're latency sensitive--if we wait, we introduce false
      ; concurrency.
      (if-let [op' (.poll completions poll-timeout TimeUnit/MICROSECONDS)]
        (let [thread (process->thread ctx (:process op'))
              time    (util/relative-time-nanos)
              ; Update context with new time and thread being free
              ctx     (-> ctx
                          (assoc :time time)
                          (c/update :free-threads conj thread))
              ; Workers that crash (other than the nemesis) should be assigned
              ; new thread identifiers.
              ctx     (if (or (= :nemesis thread) (not= :info (:type op')))
                        ctx
                        (c/update ctx :workers assoc thread
                                  (next-process ctx thread)))
              ; Let generator know about our completion
              gen     (update gen test ctx op')
              history (if (goes-in-history? op')
                        (conj! history op')
                        history)]
          ; Log completion in history and move on!
          (recur ctx gen (dec outstanding) 0 history))

        ; There's nothing to complete; let's see what the generator's up to
        (let [time        (util/relative-time-nanos)
              ctx         (assoc ctx :time time)
              [op gen']   (op gen test ctx)]
          (condp = op
            ; We're exhausted, but workers might still be going.
            nil (if (pos? outstanding)
                  ; Still waiting on workers
                  (recur ctx gen outstanding (long max-pending-interval)
                         history)
                  ; Good, we're done. Tell workers to exit...
                  (do (doseq [[thread queue] invocations]
                        (.put ^ArrayBlockingQueue queue {:type :exit}))
                      ; Wait for exit
                      (mapv (comp deref :future) workers)
                      (persistent! history)))

            ; Nothing we can do right now. Let's try to complete something.
            :pending (recur ctx gen outstanding (long max-pending-interval)
                            history)

            ; Good, we've got an invocation.
            (if (< time (:time op))
              ; Can't evaluate this op yet!
              (recur ctx gen outstanding
                         ; Unless something changes, we don't need to ask the
                         ; generator for another op until it's time.
                         (long (/ (- (:time op) time) 1000))
                         history)

              ; Good, we can run this.
              (let [thread (process->thread ctx (:process op))
                    ; Dispatch it to a worker as quick as we can
                    _ (.put ^ArrayBlockingQueue (get invocations thread) op)
                    ; Update our context to reflect
                    ctx (-> ctx
                            (assoc :time (:time op)) ; Use time instead?
                            (c/update :free-threads disj thread))
                    ; Let the generator know about the invocation
                    gen' (update gen' test ctx op)
                    history (if (goes-in-history? op)
                              (conj! history op)
                              history)]
                (recur ctx gen' (inc outstanding) 0 history)))))))))

;; Generators!

(extend-protocol Generator
  nil
  (update [gen test ctx event] nil)
  (op [this test ctx] nil)

  clojure.lang.IPersistentMap
  (update [this test ctx event] this)
  (op [this test ctx]
    [(if-let [p (some-free-process ctx)]
       ; Automatically assign type, time, and process from the context, if not
       ; provided.
       (cond-> this
         (nil? (:time this))     (assoc :time (:time ctx))
         (nil? (:process this))  (assoc :process p)
         (nil? (:type this))     (assoc :type :invoke))

       ; No process free to accept our request
       :pending)
     this])

  clojure.lang.Seqable
  (update [this test ctx event]
    ; Updates are passed to the first generator in the sequence.
    (cons (update (first this) test ctx event) (next this)))

  (op [this test ctx]
    (when (seq this) ; Once we're out of generators, we're done
      (let [gen (first this)]
        (if-let [[op gen'] (op gen test ctx)]
          ; OK, our first gen has an op for us.
          [op (cons gen' (next this))]
          ; This generator is exhausted; move on
          (recur (next this) test ctx))))))

(extend-protocol Generator
  clojure.lang.AFunction
  (update [f test ctx event] f)
  (op [f test ctx]
    (when-let [x (if (= 2 (first (util/arities (class f))))
                   (f test ctx)
                   (f))]
      (condp instance? x
        ; Ask the map to generate an operation for us.
        clojure.lang.IPersistentMap     [(first (op x test ctx)) f]
        ; Return the (presumably a pair) directly
        clojure.lang.IPersistentVector  x
        ; ???
        (throw+ {:type :unexpected-return
                 :value x})))))

(defrecord Validate [gen]
  Generator
  (op [_ test ctx]
    (when-let [res (op gen test ctx)]
      (let [problems
            (if-not (and (vector? res) (= 2 (count res)))
              [(str "should return a vector of two elements.")]
              (let [[op gen'] res]
                  (if (= :pending op)
                               []
                               (cond-> []
                                 (not (map? op))
                                 (conj "should be either :pending or a map")

                                 (not= :invoke (:type op))
                                 (conj ":type should be :invoke")

                                 (not (number? (:time op)))
                                 (conj ":time is not a number")

                                 (not (:process op))
                                 (conj "no :process")

                                 (not-any? #{(:process op)}
                                           (free-processes ctx))
                                 (conj (str "process " (pr-str (:process op))
                                            " is not free"))))))]
        (when (seq problems)
          (binding [*print-length* 10]
            (throw+ {:type      :invalid-op
                     :generator gen
                     :context   ctx
                     :res       res
                     :problems  problems}))))
      [(first res) (Validate. (second res))]))

  (update [this test ctx event]
    (Validate. (update gen test ctx event))))

(defn validate
  "Validates the well-formedness of operations emitted from the underlying
  generator."
  [gen]
  (Validate. gen))

(defrecord Trace [k gen]
  Generator
  (op [_ test ctx]
    (try (let [[op gen'] (op gen test ctx)]
           (info k :op test ctx op gen')
           [op (when gen' (Trace. k gen'))])
         (catch Throwable t
           (info k :op test ctx :threw)
           (throw t))))

  (update [_ test ctx event]
    (try (let [gen' (update gen test ctx event)]
           (info k :update test ctx event gen')
           (when gen' (Trace. k gen')))
         (catch Throwable t
           (info k :update test ctx event :threw)
           (throw t)))))

(defn trace
  "Wraps a generator, logging calls to op and update before passing them on to
  the underlying generator. Takes a key k, which is included in every log
  line."
  [k gen]
  (Trace. k gen))

(defrecord Map [f gen]
  Generator
  (op [_ test ctx]
    (when-let [[op gen'] (op gen test ctx)]
      [(if (= :pending op) op (f op))
       (Map. f gen')]))

  (update [_ test ctx event]
    (Map. f (update gen test ctx event))))

(defn map
  "A generator which wraps another generator g, transforming operations it
  generates with (f op). When the underlying generator yields :pending or nil,
  this generator does too, without calling `f`. Passes updates to underlying
  generator."
  [f gen]
  (Map. f gen))

(defn f-map
  "Takes a function `f-map` converting op functions (:f op) to other functions,
  and a generator `g`. Returns a generator like `g`, but where fs are replaced
  according to `f-map`. Useful for composing generators together for use with a
  composed nemesis."
  [f-map g]
  (map (fn transform [op] (c/update op :f f-map)) g))


(defrecord Filter [f gen]
  Generator
  (op [_ test ctx]
    (loop [gen gen]
      (when-let [[op gen'] (op gen test ctx)]
        (if (or (= :pending op) (f op))
          ; We can let this through
          [op (Filter. f gen')]
          ; Next op!
          (recur gen')))))

  (update [_ test ctx event]
    (Filter. f (update gen test ctx event))))

(defn filter
  "A generator which filters operations from an underlying generator, passing
  on only those which match (f op). Like `map`, :pending and nil operations
  bypass the filter."
  [f gen]
  (Filter. f gen))

(defrecord IgnoreUpdates [gen]
  Generator
  (op [this test ctx]
    (op gen test ctx))

  (update [this _ _ _]
    this))

(defn ignore-updates
  "Wraps a generator. Any call to `update` is ignored, returning this
  generator with no changes.

  It's not clear if this actually confers any performance advantage right now,
  but I'm putting it here so that if someone later discovers a performance cost
  they'd rather avoid, it'll be ready for them. :)"
  [gen]
  (IgnoreUpdates. gen))

(defn on-threads-context
  "Helper function to transform contexts for OnThreads. Takes a function which
  returns true if a thread should be included in the context."
  [f ctx]
  (let [; Filter free threads to just those we want
        ctx     (c/update ctx :free-threads (partial c/filter f))
        ; Update workers to remove threads we won't use
        ctx (->> (:workers ctx)
                 (c/filter (comp f key))
                 (into {})
                 (assoc ctx :workers))]
    ctx))

(defrecord OnThreads [f gen]
  Generator
  (op [this test ctx]
    (when-let [[op gen'] (op gen test (on-threads-context f ctx))]
      [op (OnThreads. f gen')]))

  (update [this test ctx event]
    (if (f (process->thread ctx (:process event)))
      (OnThreads. f (update gen test (on-threads-context f ctx) event))
      this)))

(defn on-threads
  "Wraps a generator, restricting threads which can use it to only those
  threads which satisfy (f thread). Alters the context passed to the underlying
  generator: it will only include free threads and workers satisfying f.
  Updates are passed on only when the thread performing the update matches f."
  [f gen]
  (OnThreads. f gen))

(def on "For backwards compatibility" on-threads)

(defn soonest-op-vec
  "Takes two [op, ...] vectors, and returns the vector whose op occurs first.
  Op maps occur before those which are :pending. :pending occurs before `nil`.

  We use vectors here because you may want to pass [op, gen'] pairs, or
  possibly encode additional information into the vector, so you can, for
  instance, identify *which* of several generators was the next one."
  [pair1 pair2]
  (condp = nil
    pair1 pair2
    pair2 pair1
    (let [op1 (first pair1)
          op2 (first pair2)]
      (condp = :pending
        op1 pair2
        op2 pair1
        (if (<= (:time op1) (:time op2))
          pair1
          pair2)))))

(defrecord Any [gens]
  Generator
  (op [this test ctx]
    (when-let [[op gen' i] (->> gens
                                (map-indexed
                                  (fn [i gen]
                                    (when-let [pair (op gen test ctx)]
                                      (conj pair i))))
                                (reduce soonest-op-vec nil))]
      [op (Any. (assoc gens i gen'))]))

  (update [this test ctx event]
    (Any. (mapv (fn updater [gen] (update gen test ctx event)) gens))))

(defn any
  "Takes multiple generators and binds them together. Operations are taken from
  any generator. Updates are propagated to all generators."
  [& gens]
  (condp = (count gens)
    0 nil
    1 (first gens)
      (Any. (vec gens))))

(defrecord EachThread [fresh-gen gens]
  ; fresh-gen is a generator we use to initialize a thread's state, the first
  ; time we see it.
  ; gens is a map of threads to generators.
  Generator
  (op [this test ctx]
    (let [free-threads (free-threads ctx)
          all-threads  (all-threads ctx)
          [op gen' thread :as soonest]
          (->> free-threads
               (keep (fn [thread]
                      (let [gen (get gens thread fresh-gen)
                            process (get (:workers ctx) thread)
                            ; Give this generator a context *just* for one
                            ; thread
                            ctx (assoc ctx
                                       :free-threads [thread]
                                       :workers {thread process})]
                        (when-let [pair (op gen test ctx)]
                          (conj pair thread)))))
               (reduce soonest-op-vec nil))]
      (cond ; A free thread has an operation
            soonest [op (EachThread. fresh-gen (assoc gens thread gen'))]

            ; Some thread is busy; we can't tell what to do just yet
            (not= (count free-threads) (count all-threads))
            [:pending this]

            ; Every thread is exhausted
            true
            nil)))

  (update [this test ctx event]
    (let [process (:process event)
          thread (process->thread ctx process)
          gen    (get gens thread fresh-gen)
          ctx    (-> ctx
                     (c/update :free-threads (partial c/filter #{thread}))
                     (assoc :workers {thread process}))
          gen'   (update gen test ctx event)]
      (EachThread. fresh-gen (assoc gens thread gen')))))

(defn each-thread
  "Takes a generator. Constructs a generator which maintains independent copies
  of that generator for every thread. Each generator sees exactly one thread in
  its free process list. Updates are propagated to the generator for the thread
  which emitted the operation."
  [gen]
  (EachThread. gen {}))


(defrecord Reserve [ranges all-ranges gens]
  ; ranges is a collection of sets of threads engaged in each generator.
  ; all-ranges is the union of all ranges.
  ; gens is a vector of generators corresponding to ranges, followed by the
  ; default generator.
  Generator
  (op [_ test ctx]
    (let [[op gen' i :as soonest]
          (->> ranges
               (map-indexed
                 (fn [i threads]
                   (let [gen (nth gens i)
                         ; Restrict context to this range of threads
                         ctx (on-threads-context threads ctx)]
                     ; Ask this range's generator for an op
                     (when-let [pair (op gen test ctx)]
                       ; Remember our index
                       (conj pair i)))))
               ; And for the default generator, compute a context without any
               ; threads from defined ranges...
               (cons (let [ctx (on-threads-context (complement all-ranges) ctx)]
                       ; And construct a triple for the default generator
                       (when-let [pair (op (peek gens) test ctx)]
                         (conj pair (count ranges)))))
               (reduce soonest-op-vec nil))]
      (when soonest
        ; A range has an operation to do!
        [op (Reserve. ranges all-ranges (assoc gens i gen'))])))

  (update [this test ctx event]
    (let [process (:process event)
          thread  (process->thread ctx process)
          ; Find generator whose thread produced this event.
          i (reduce (fn red [i range]
                      (if (range thread)
                        (reduced i)
                        (inc i)))
                    0
                    ranges)]
      (Reserve. ranges all-ranges (c/update gens i update test ctx event)))))

(defn reserve
  "Takes a series of count, generator pairs, and a final default generator.

  (reserve 5 write 10 cas read)

  The first 5 threads will call the `write` generator, the next 10 will emit
  CAS operations, and the remaining threads will perform reads. This is
  particularly useful when you want to ensure that two classes of operations
  have a chance to proceed concurrently--for instance, if writes begin
  blocking, you might like reads to proceed concurrently without every thread
  getting tied up in a write.

  Each generator sees a context which only includes the worker threads which
  will execute that particular generator. Updates from a thread are propagated
  only to the generator which that thread executes."
  [& args]
  (let [gens (->> args
                  drop-last
                  (partition 2)
                  ; Construct [thread-set gen] tuples defining the range of
                  ; thread indices covering a given generator, lower
                  ; inclusive, upper exclusive.
                  (reduce (fn [[n gens] [thread-count gen]]
                            (let [n' (+ n thread-count)]
                              [n' (conj gens [(set (range n n')) gen])]))
                          [0 []])
                  second)
        ranges      (mapv first gens)
        all-ranges  (reduce set/union ranges)
        gens        (mapv second gens)
        default     (last args)
        gens        (conj gens default)]
    (assert default)
    (Reserve. ranges all-ranges gens)))

(declare nemesis)

(defn clients
  "In the single-arity form, wraps a generator such that only clients
  request operations from it. In its two-arity form, combines a generator of
  client operations and a generator for nemesis operations into one. When the
  process requesting an operation is :nemesis, routes to the nemesis generator;
  otherwise to the client generator."
  ([client-gen]
   (on (complement #{:nemesis}) client-gen))
  ([client-gen nemesis-gen]
   (any (clients client-gen)
        (nemesis nemesis-gen))))

(defn nemesis
  "In the single-arity form, wraps a generator such that only the nemesis
  requests operations from it. In its two-arity form, combines a generator of
  client operations and a generator for nemesis operations into one. When the
  process requesting an operation is :nemesis, routes to the nemesis generator;
  otherwise to the client generator."
  ([nemesis-gen]
   (on #{:nemesis} nemesis-gen))
  ([nemesis-gen client-gen]
   (any (nemesis nemesis-gen)
        (clients client-gen))))

(defn dissoc-vec
  "Cut a single index out of a vector, returning a vector one shorter, without
  the element at that index."
  [v i]
  (into (subvec v 0 i)
        (subvec v (inc i))))

(defrecord Mix [i gens]
  ; i is the next generator index we intend to work with; we reset it randomly
  ; when emitting ops.
  Generator
  (op [_ test ctx]
    (when (seq gens)
      (if-let [[op gen'] (op (nth gens i) test ctx)]
        ; Good, we have an op
        [op (Mix. (rand-int (count gens)) (assoc gens i gen'))]
        ; Oh, we're out of ops on this generator. Compact and recur.
        (op (Mix. (rand-int (dec (count gens))) (dissoc-vec gens i))
            test ctx))))

  (update [this test ctx event]
    this))

(defn mix
  "A random mixture of several generators. Takes a collection of generators and
  chooses between them uniformly. Ignores updates; some users create broad
  (hundreds of generators) mixes.

  To be precise, a mix behaves like a sequence of one-time, randomly selected
  generators from the given collection. This is efficient and prevents multiple
  generators from competing for the next slot, making it hard to control the
  mixture of operations."
  [gens]
  (Mix. (rand-int (count gens)) gens))


(defrecord Limit [remaining gen]
  Generator
  (op [_ test ctx]
    (when (pos? remaining)
      (when-let [[op gen'] (op gen test ctx)]
        [op (Limit. (dec remaining) gen')])))

  (update [this test ctx event]
    (Limit. remaining (update gen test ctx event))))

(defn limit
  "Wraps a generator and ensures that it returns at most `limit` operations.
  Propagates every update to the underlying generator."
  [remaining gen]
  (Limit. remaining gen))

(defn once
  "Emits only a single item from the underlying generator."
  [gen]
  (limit 1 gen))

(defn log
  "A generator which, when asked for an operation, logs a message and yields
  nil."
  [msg]
  (once {:type :log, :value msg}))

(defrecord Repeat [remaining gen]
  ; Remaining is positive for a limit, or -1 for infinite repeats.
  Generator
  (op [_ test ctx]
    (when-not (zero? remaining)
      (when-let [[op gen'] (op gen test ctx)]
        ; If you actually hit MIN_INT doing this... you probably have bigger
        ; problems on your hands.
        [op (Repeat. (dec remaining) gen)])))

  (update [this test ctx event]
    (Repeat. remaining (update gen test ctx event))))

(defn repeat
  "Wraps a generator so that it emits operations infinitely, or, with an
  initial limit, up to `limit` times. Think of this as the inverse of `once`:
  where `once` takes a generator that emits many things and makes it emit one,
  this takes a generator that emits (presumably) one thing, and makes it emit
  many.

  The state of the underlying generator is unchanged as `repeat` yields
  operations, but `repeat` does *not* memoize its results; repeating a
  nondeterministic generator results in a sequence of *different* operations."
  ([gen]
   (Repeat. -1 gen))
  ([limit gen]
   (assert (not (neg? limit)))
   (Repeat. limit gen)))

(defrecord ProcessLimit [n procs gen]
  Generator
  (op [_ test ctx]
    (when-let [[op gen'] (op gen test ctx)]
      (if (= :pending op)
        [op (ProcessLimit. n procs gen')]
        (let [procs' (into procs (all-processes ctx))]
          (when (<= (count procs') n)
            [op (ProcessLimit. n procs' gen')])))))

  (update [_ test ctx event]
    (ProcessLimit. n procs (update gen test ctx event))))

(defn process-limit
  "Takes a generator and returns a generator with bounded concurrency--it emits
  operations for up to n distinct processes, but no more.

  Specifically, we track the set of all processes in a context's `workers` map:
  the underlying generator can return operations only from contexts such that
  the union of all processes across all such contexts has cardinality at most
  `n`. Tracking the union of all *possible* processes, rather than just those
  processes actually performing operations, prevents the generator from
  \"trickling\" at the end of a test, i.e. letting only one or two processes
  continue to perform ops, rather than the full concurrency of the test."
  [n gen]
  (ProcessLimit. n #{} gen))

(defrecord TimeLimit [limit cutoff gen]
  Generator
  (op [_ test ctx]
    (let [[op gen'] (op gen test ctx)]
      (case op
        ; We're exhausted
        nil       nil
        ; We're pending!
        :pending  [:pending (TimeLimit. limit cutoff gen')]
        ; We have an op; lazily initialize our cutoff and check to see if it's
        ; past.
        (let [cutoff (or cutoff (+ (:time op) limit))]
          (when (< (:time op) cutoff)
            [op (TimeLimit. limit cutoff gen')])))))

  (update [this test ctx event]
    (TimeLimit. limit cutoff (update gen test ctx event))))

(defn time-limit
  "Takes a time in seconds, and an underlying generator. Once this emits an
  operation (taken from that underlying generator), will only emit operations
  for dt seconds."
  [dt gen]
  (TimeLimit. (long (util/secs->nanos dt)) nil gen))

(defrecord Stagger [dts next-time gen]
  Generator
  (op [this test ctx]
    (when-let [[op gen'] (op gen test ctx)]
      (cond ; No need to do anything to pending ops
            (= :pending op)
            [op this]

            ; We're ready to issue this operation.
            (<= next-time (:time op))
            [op (Stagger. (next dts) (+ next-time (first dts)) gen')]

            ; Not ready yet
            true
            [(assoc op :time next-time)
             (Stagger. (next dts) (+ next-time (first dts)) gen')])))


  (update [_ test ctx event]
    (Stagger. dts next-time (update gen test ctx event))))

(defn stagger
  "Wraps a generator. Operations from that generator are scheduled at uniformly
  random intervals between 0 to 2 * dt, or, if we can't keep up, as fast as
  possible. There's... an argument that maybe we should limit the amount of
  catching-up this generator performs, but I'm not sure if that's a practiacl
  concern yet.

  Unlike Jepsen's original version of `stagger`, this actually *means*
  'schedule at roughly every dt seconds', rather than 'introduce roughly dt
  seconds of latency between ops', which makes this less sensitive to request
  latency variations.

  Also note that unlike Jepsen's original version of `stagger`, this delay
  applies to *all* operations, not to each thread independently. If your old
  stagger dt is 10, and your concurrency is 5, your new stagger dt should be
  2."
  [dt gen]
  (let [dt (util/secs->nanos (* 2 dt))]
    (Stagger. (repeatedly (comp long (partial rand dt))) 0 gen)))

; This isn't actually DelayTil. It spreads out *all* requests evenly. Feels
; like it might be useful later.
;(defrecord DelayTil [dt anchor wait-til gen]
;  Generator
;  (op [_ test ctx]
;    (when-let [[op gen'] (op gen test ctx)]
;      (if (= :pending op)
;        ; You can't delay what's pending!
;        [op (DelayTil. dt anchor wait-til gen')]

;        ; OK we have an actual op
;        (let [; The next op should occur at time t
;              t         (if wait-til
;                          (max wait-til (:time op))
;                          (:time op))
;              ; Update op
;              op'       (assoc op :time t)
;              ; Initialize our anchor if we don't have one
;              anchor'   (or anchor t)
;              ; And compute the next wait-til time after t
;              wait-til' (+ t (- dt (mod (- t anchor') dt)))
;              ; Our next generator state
;              gen'      (DelayTil. dt anchor' wait-til' gen')]
;          [op' gen']))))

;  (update [this test ctx event]
;    (DelayTil. dt anchor wait-til (update gen test ctx event))))

;(defn delay-til
;  "Given a time dt in seconds, and an underlying generator gen, constructs a
;  generator which emits operations such that successive invocations are at
;  least dt seconds apart."
;  [dt gen]
;  (DelayTil. (long (util/secs->nanos dt)) nil nil gen))

(defrecord DelayTil [dt anchor gen]
  Generator
  (op [_ test ctx]
    (when-let [[op gen'] (op gen test ctx)]
      (if (= op :pending)
        ; Just pass these through; we don't know when they'll occur!
        [op (DelayTil. dt anchor gen')]

        ; OK we have an actual op. Compute its new event time.
        (let [t      (:time op)
              anchor (or anchor t)
              ; A helpful way to test this at the REPL:
              ; (let [anchor 0 dt 3]
              ;   (->> (range 20)
              ;        (map (fn [t]
              ;          [t (+ t (mod (- dt (mod (- t anchor) dt)) dt))]))))
              ; We do a second mod here because mod has an off-by-one
              ; problem in this form; it'll compute offsets that push 10 -> 15,
              ; rather than letting 10->10.
              t      (+ t (mod (- dt (mod (- t anchor) dt)) dt))]
          [(assoc op :time t) (DelayTil. dt anchor gen')]))))

  (update [this test ctx event]
    (DelayTil. dt anchor (update gen test ctx event))))

(defn delay-til
  "Given a time dt in seconds, and an underlying generator gen, constructs a
  generator which aligns invocations to intervals of dt seconds."
  [dt gen]
  (DelayTil. (long (util/secs->nanos dt)) nil gen))

(defn sleep
  "Emits a special operation which causes its receiving process to do nothing
  for dt seconds."
  [dt]
  {:type :sleep, :value dt})

(defrecord Synchronize [gen]
  Generator
  (op [this test ctx]
    (let [free (free-threads ctx)
          all  (all-threads ctx)]
      (if (and (= (count free)
                  (count all))
               (= (set free)
                  (set all)))
        ; We're ready, replace ourselves with the generator
        (op gen test ctx)
        ; Not yet
        [:pending this])))

  (update [_ test ctx event]
    (Synchronize. (update gen test ctx event))))

(defn synchronize
  "Takes a generator, and waits for all workers to be free before it begins."
  [gen]
  (Synchronize. gen))

(defn phases
  "Takes several generators, and constructs a generator which evaluates
  everything from the first generator, then everything from the second, and so
  on."
  [& generators]
  (c/map synchronize generators))

(defn then
  "Generator A, synchronize, then generator B. Note that this takes its
  arguments backwards: b comes before a. Why? Because it reads better in ->>
  composition. You can say:

      (->> (fn [] {:f :write :value 2})
           (limit 3)
           (then (once {:f :read})))"
  [a b]
  [b (synchronize a)])

(defrecord UntilOk [gen done?]
  Generator
  (op [this test ctx]
    (when-not done?
      (when-let [[op gen'] (op gen test ctx)]
        [op (UntilOk. gen' done?)])))

  (update [this test ctx event]
    (if (= :ok (:type event))
      ; We're finished; no need to update any more!
      (assoc this :done? true)
      ; Propagate update down
      (c/update this :gen update test ctx event))))

(defn until-ok
  "Wraps a generator, yielding operations from it until one operation completes
  with :type :ok."
  [gen]
  (UntilOk. gen false))
