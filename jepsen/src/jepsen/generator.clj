(ns jepsen.generator
  "# In a Nutshell

  Generators tell Jepsen what to do during a test. Generators are purely
  functional objects which support two functions: `op` and `update`. `op`
  produces operations for Jepsen to perform: it takes a test and context
  object, and yields:

  - nil if the generator is exhausted
  - :pending if the generator doesn't know what to do yet
  - [op, gen'], where op' is the next operation this generator would like to
  execute, and `gen'` is the state of the generator that would result if `op`
  were evaluated. Ops must be a jepsen.history.Op.

  `update` allows generators to evolve as events occur--for instance, when an
  operation is invoked or completed. For instance, `update` allows a generator
  to emit read operations *until* at least one succeeds.

  Maps, sequences, and functions are all generators, allowing you to write all
  kinds of generators using existing Clojure tooling. This namespace provides
  additional transformations and combinators for complex transformations.

  # Migrating From Classic Generators

  The old jepsen.generator namespace used mutable state everywhere, and was
  plagued by race conditions. jepsen.generator.pure provides a similar API, but
  its purely functional approach has several advantages:

  - Pure generators shouldn't deadlock or throw weird interrupted exceptions.
    These issues have plagued classic generators; I've done my best to make
    incremental improvements, but the problems seem unavoidable.

  - Pure generators can respond to completion operations, which means you can
    write things like 'keep trying x until y occurs' without sharing complex
    mutable state with clients.

  - Sequences are pure generators out of the box; no more juggling gen/seq
    wrappers. Use existing Clojure sequence transformations to build complex
    behaviors.

  - Pure generators provide an explicit 'I don't know yet' state, which is
    useful when you know future operations might come, but don't know when or
    what they are.

  - Pure generators do not rely on dynamic state; their arguments are all
    explicit. They are deterministically testable.

  - Pure generators allow new combinators like (any gen1 gen2 gen3), which
    returns the first operation from any of several generators; this approach
    was impossible in classic generators.

  - Pure generators have an explicit, deterministic model of time, rather than
    relying on thread scheduler constructs like Thread/sleep.

  - Certain constructs, like gen/sleep and gen/log in classic generators, could
    not be composed in sequences readily; pure generators provide a regular
    composition language.

  - Constructs like gen/each, which were fragile in classic generators and
    relied on macro magic, are now simple functions.

  - Pure generators are significantly simpler to implement and test than
    classic generators, though they do require careful thought.

  There are some notable tradeoffs, including:

  - Pure generators perform all generator-related computation on a single
    thread, and create additional garbage due to their pure functional approach.
    However, realistic generator tests yield rates over 20,000 operations/sec,
    which seems more than sufficient for Jepsen's purposes.

  - The API is subtly different. In my experience teaching hundreds of
    engineers to write Jepsen tests, users typically cite the generator API as
    one of Jepsen's best features. I've tried to preserve as much of its shape
    as possible, while sanding off rough edges and cleaning up inconsistencies.
    Some functions have the same shape but different semantics: `stagger`, for
    instance, now takes a *total* rather than a `*per-thread*` rate. Some
    infrequently-used generators have not been ported, to keep the API smaller.

  - `update` and contexts are not a full replacement for mutable state. We
    think they should suffice for most practical uses, and controlled use of
    mutable shared state is still possible.

  - You can (and we encourage!) the use of impure functions, e.g. randomness,
    as impure generators. However, it's possible I haven't fully thought
    through the implications of this choice; the semantics may evolve over
    time.

  When migrating old to new generators, keep in mind:

  - `gen/seq` and `gen/seq-all` are unnecessary; any Clojure sequence is
    already a pure generator. `gen/seq` didn't just turn sequences into
    generators; it also ensured that only one operation was consumed from each.
    This is now explicit: use `(map gen.pure/once coll)` instead of (gen/seq
    coll)`, and `coll` instead of `(gen/seq-all coll)`. Where the sequence is
    of one-shot generators already, there's no need to wrap elements with
    gen/once: instead of `(gen/seq [{:f :read} {:f :write}])`), you can write
    [{:f :read} {:f :write}] directly.

  - Functions return generators, not just operations, which makes it easier to
    express sequences of operations like 'pick the current leader, isolate it,
    then kill that same node, then restart that node.' Use `#(gen/once {:f
    :write, :value (rand-int 5))` instead of `(fn [] {:f :write, :value
    (rand-int 5)})`.

  - `stagger`, `delay`, etc. now take total rates, rather than the rate per
    thread.

  - `delay-til` is gone. It should come back; I just haven't written it yet.
    Defining what exactly delay-til means is... surprisingly tricky.

  - `each` used to mean 'on each process', but in practice what users generally
    wanted was 'on each thread'--on each process had a tendency to result in
    unexpected infinite loops when ops crashed. `each-thread` is probably what
    you want instead.

  - Instead of using *jepsen.generator/threads*, etc, use helper functions like
    some-free-process.

  - Functions now take zero args (f) or a test and context map (f test ctx),
    rather than (f test process).

  - Maps are one-shot generators by default, rather than emitting themselves
    indefinitely. This streamlines the most common use cases:

      - (map (fn [x] {:f :write, :value x}) (range)) produces a series of
        distinct, monotonically increasing writes

      - (fn [] {:f :inc, :value (rand-nth 5)}) produces a series of random
        increments, rather than a series where every value is the *same*
        (randomly selected) value.

    When migrating, you can drop most uses of gen/once around maps, and
    introduce (repeat ...) where you want to repeat an operation more than once.

  # In More Detail

  A Jepsen history is a list of operations--invocations and completions. A
  generator's job is to specify what invocations to perform, and when. In a
  sense, a generator *becomes* a history as Jepsen incrementally applies it to
  a database.

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

  ## By Example

  Perform a single read

    {:f :read}

  Perform a single random write:

    (fn [] {:f :write, :value (rand-int 5))

  Perform 10 random writes. This is regular clojure.core/repeat:

    (repeat 10 (fn [] {:f :write, :value (rand-int 5)))

  Perform a sequence of 50 unique writes. We use regular Clojure sequence
  functions here:

    (->> (range)
         (map (fn [x] {:f :write, :value (rand-int 5)}))
         (take 50))

  Write 3, then (possibly concurrently) read:

    [{:f :write, :value 3} {:f :read}]

  Since these might execute concurrently, the read might not observe the write.
  To wait for the write to complete first:

    (gen/phases {:f :write, :value 3}
                {:f :read})

  Have each thread independently perform a single increment, then read:

    (gen/each-thread [{:f :inc} {:f :read}])

  Reserve 5 threads for reads, 10 threads for increments, and the remaining
  threads reset a counter.

    (gen/reserve 5  (repeat {:f :read})
                 10 (repeat {:f :inc})
                    (repeat {:f :reset}))

  Perform a random mixture of unique writes and reads, randomly timed, at
  roughly 10 Hz, for 30 seconds:

    (->> (gen/mix [(repeat {:f :read})
                   (map (fn [x] {:f :write, :value x}) (range))])
         (gen/stagger 1/10)
         (gen/time-limit 30))

  While that's happening, have the nemesis alternate between
  breaking and repairing something roughly every 5 seconds:

    (->> (gen/mix [(repeat {:f :read})
                   (map (fn [x] {:f :write, :value x}) (range))])
         (gen/stagger 1/10)
         (gen/nemesis (->> (cycle [{:f :break}
                                   {:f :repair}])
                           (gen/stagger 5)))
         (gen/time-limit 30))

  Follow this by a single nemesis repair (along with an informational log
  message), wait 10 seconds for recovery, then have each thread perform reads
  until that thread sees at least one OK operation.

    (gen/phases (->> (gen/mix [(repeat {:f :read})
                               (map (fn [x] {:f :write, :value x}) (range))])
                     (gen/stagger 1/10)
                     (gen/nemesis (->> (cycle [{:f :break}
                                               {:f :repair}])
                                       (gen/stagger 5)))
                     (gen/time-limit 30))
                (gen/log \"Recovering\")
                (gen/nemesis {:f :repair})
                (gen/sleep 10)
                (gen/log \"Final read\")
                (gen/clients (gen/each-thread (gen/until-ok {:f :read}))))

  ## Contexts

  A *context* is a map which provides information about the state of the world
  to generators. For instance, a generator might need to know the number of
  threads which will ask it for operations. It can get that number from the
  *context*. Users can add their own values to the context map, which allows
  two generators to share state. When one generator calls another, it can pass
  a modified version of the context, which allows us to write generators that,
  say, run two independent workloads, each with their own concurrency and
  thread mappings.

  The standard context mappings, which are provided by Jepsen when invoking the
  top-level generator, and can be expected by every generator, are defined in
  jepsen.generator.context. They include some stock fields:

      :time           The current Jepsen linear time, in nanoseconds

  Additional fields (e.g. :threads, :free-threads, etc) are present for
  bookkeeping, but should not be interfered with or accessed directly: contexts
  are performance-sensitive and for optimization reasons their internal
  structure is somewhat complex. Use the functions `all-threads`,
  `thread->process`, `some-free-process`, etc. See jepsen.generator.context for
  these functions, which are also imported here in jepsen.generator.

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
  - The free processes set reflects the state after the event has taken place;
    e.g. if the event is an invoke, the thread is listed as no longer free; if
    the event is a completion, the thread is listed as free.
  - The worker map reflects the process which that thread worker was executing
    at the time the event occurred.

  See jepsen.generator.context for more.

  ## Default implementations

  Nil is a valid generator; it ignores updates and always yields nil for
  operations.

  IPersistentMaps are generators which ignore updates and return exactly one
  operation which looks like the map itself, but with default values for time,
  process, and type provided based on the context. This means you can write a
  generator like

    {:f :write, :value 2}

  and it will generate a single op like

    {:type :invoke, :process 3, :time 1234, :f :write, :value 2}

  To produce an infinite series of ops drawn from the same map, use

    (repeat {:f :write, :value 2}).

  Sequences are generators which assume the elements of the sequence are
  themselves generators. They ignore updates, and return all operations from
  the first generator in the sequence, then all operations from the second, and
  so on.

  Functions are generators which ignore updates and can take either test and
  context as arguments, or no args. Functions should be *mostly* pure, but some
  creative impurity is probably OK. For instance, returning randomized :values
  for maps is probably all right. I don't know the laws! What is this, Haskell?

  When a function is used as a generator, its return value is used as a
  generator; that generator is used until exhausted, and then the function is
  called again to produce a new generator. For instance:

    ; Produces a series of different random writes, e.g. 1, 5, 2, 3...
    (fn [] {:f :write, :value (rand-int 5)})

    ; Alternating write/read ops, e.g. write 2, read, write 5, read, ...
    (fn [] (map gen/once [{:f :write, :value (rand-int 5)}
                          {:f :read}]))

  Promises and delays are generators which ignore updates, yield :pending until
  realized, then are replaced by whatever generator they contain. Delays are
  not evaluated until they *could* produce an op, so you can include them in
  sequences, phases, etc., and they'll be evaluated only once prior ops have
  been consumed."
  (:refer-clojure :exclude [await concat cycle delay filter map repeat run! update])
  (:require [clojure [core :as c]
                     [datafy :refer [datafy]]
                     [set :as set]]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn error]]
            [clojure.pprint :as pprint :refer [pprint]]
            [dom-top.core :refer [loopr]]
            [fipp.ednize :as fipp.ednize]
            [jepsen [history :as history]
                    [util :as util]]
            [jepsen.generator.context :as context]
            [potemkin :refer [import-vars]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (io.lacuna.bifurcan Set)
           (java.util ArrayList)))

;; These used to be a part of jepsen.generator directly, and it makes sense for
;; users to interact with them here. For cleanliness, they actually live in
;; jepsen.generator.context.
(import-vars [jepsen.generator.context
              all-processes
              all-threads
              context
              free-processes
              free-threads
              process->thread
              some-free-process
              thread->process])

(defprotocol Generator
  (update [gen test context event]
          "Updates the generator to reflect an event having taken place.
          Returns a generator (presumably, `gen`, perhaps with some changes)
          resulting from the update.")

  (op [gen test context]
      "Obtains the next operation from this generator. Returns an pair
      of [op gen'], or [:pending gen], or nil if this generator is exhausted."))

;; Pretty-printing

(defmethod pprint/simple-dispatch jepsen.generator.Generator
  [gen]
  (if (map? gen)
    (do (.write ^java.io.Writer *out* (str (.getName (class gen)) "{"))
        (pprint/pprint-newline :mandatory)
        (let [prefix "  "
              suffix "}"]
          (pprint/pprint-logical-block :prefix prefix :suffix suffix
            (pprint/print-length-loop [aseq (seq gen)]
              (when aseq
                (pprint/pprint-logical-block
                  (pprint/write-out (key (first aseq)))
                  (.write ^java.io.Writer *out* " ")
                  (pprint/pprint-newline :miser)
                  (pprint/write-out (fnext (first aseq))))
                (when (next aseq)
                  (.write ^java.io.Writer *out* ", ")
                  (pprint/pprint-newline :linear)
                  (recur (next aseq))))))))

    ; Probably a reify or something weird we can't print
    (prn gen)))

(prefer-method pprint/simple-dispatch
               jepsen.generator.Generator clojure.lang.IRecord)
(prefer-method pprint/simple-dispatch
               jepsen.generator.Generator clojure.lang.IPersistentMap)

(extend-protocol fipp.ednize/IOverride (:on-interface Generator))
(extend-protocol fipp.ednize/IEdn (:on-interface Generator)
  (-edn [gen]
    (if (record? gen)
      ; Ugh this is such a hack but Fipp's extension points are sort of a
      ; mess--you can't override the document generation behavior on a
      ; per-class basis. Probably sensible for perf reasons, but makes our life
      ; hard.
      ;
      ; Convert records (which respond to map?) into maps.
      (list (symbol (.getName (class gen)))
            (into {} gen))
      ; We can't return a reify without entering an infinite loop (ugh) so uhhh
      (tagged-literal 'unprintable (str gen))
      )))

;; Fair sets
;
; Our contexts need a set of free threads which supports an efficient way of
; getting a single thread. An easy solution is to use `first` to get the first
; element of the set, but if a thread executes quickly, it's possible that the
; thread will go right *back* into the set at the first position immediately,
; which can lead to thread starvation: some workers never execute requests. We
; want a more fair scheduler, which gives each thread a uniform chance of
; executing.
;
; To do this, we use a Set from ztellman's Bifurcan collections, which supports
; efficient nth.

;; Helpers

(defn rand-int-seq
  "Generates a reproducible sequence of random longs, given a random seed. If
  seed is not provided, taken from (rand-int))."
  ([] (rand-int-seq (rand-int Integer/MAX_VALUE)))
  ([seed]
   (let [gen (java.util.Random. seed)]
     (repeatedly #(.nextLong gen)))))

;; Generators!

(defn tracking-get!
  "Takes an ArrayList, a map, a key, and a not-found value. Reads key from
  map, returning it or not-found. Adds the key to the list if it was in the
  map. Yourkit led me down this path."
  [^ArrayList read-keys m k not-found]
  (let [v (get m k ::not-found)]
    (if (identical? v ::not-found)
      not-found
      (do (.add read-keys k)
          v))))

(defn fill-in-op
  "Takes an operation as a map and fills in missing fields for :type, :process,
  and :time using context. Returns :pending if no process is free. Turns maps
  into history Ops."
  [op ctx]
  ; This will be both inefficient and wrong for Ops, but users shouldn't
  ; actually be passing those to us here.
  (assert (not (instance? jepsen.history.Op op)))
  (if-let [p (some-free-process ctx)]
    ; Automatically assign type, time, and process from the context, if not
    ; provided.
    (let [; We want to avoid using dissoc if we can POSSIBLY avoid it, so we
          ; keep track of the fields we've read from the op.
          read-keys  (ArrayList. 5)
          time       (tracking-get! read-keys op :time (:time ctx))
          type       (tracking-get! read-keys op :type :invoke)
          process    (tracking-get! read-keys op :process p)
          f          (tracking-get! read-keys op :f nil)
          value      (tracking-get! read-keys op :value nil)
          read-count (.size read-keys)
          ; Any other fields?
          ext     (if (< read-count (count op))
                    ; There's fields in the map we didn't read. Pull out the
                    ; keys we DID read
                    (loop [i 0, ext op]
                      (if (= read-count i)
                        op
                        (recur (inc i) (dissoc op (.get read-keys i)))))
                    nil)]
      (jepsen.history.Op. -1 ; Index
                          time
                          type
                          process
                          f
                          value
                          nil ; meta
                          ext))
    :pending))

(defrecord Fn
  [; We memoize the function's arity so we don't have to reflect
   ^long arity
   ; The function itself
   f]
  Generator
  (update [this test ctx event] this)

  ; When asked for an op, we invoke f to produce a generator, then exhaust that
  ; before coming back to ourselves.
  (op [this test ctx]
    (when-let [gen (if (= arity 2)
                     (f test ctx)
                     (f))]
      (op [gen this] test ctx))))

(defn fn-wrapper
  "Wraps a function into a wrapper which makes it more efficient to invoke. We
  memoize the function's arity, in particular, to reduce reflection."
  [f]
  (Fn. (first (util/arities (class f))) f))

(extend-protocol Generator
  nil
  (update [gen test ctx event] nil)
  (op [this test ctx] nil)

  clojure.lang.APersistentMap
  (update [this test ctx event] this)
  (op [this test ctx]
    (let [op (fill-in-op this ctx)]
      [op (if (= :pending op) this nil)]))

  clojure.lang.AFunction
  (update [f test ctx event]
    (update (fn-wrapper f) test ctx event))

  (op [f test ctx]
    (op (fn-wrapper f) test ctx))

  clojure.lang.Delay
  (update [d test ctx event] d)

  (op [d test ctx]
    (op @d test ctx))

  clojure.lang.Seqable
  (update [this test ctx event]
    (when (seq this)
      ; Updates are passed to the first generator in the sequence.
      (cons (update (first this) test ctx event) (next this))))

  (op [this test ctx]
    ;(binding [*print-length* 3] (prn :op this))
    (when (seq this) ; Once we're out of generators, we're done
      (let [gen (first this)]
        (if-let [[op gen'] (op gen test ctx)]
          ; OK, our first gen has an op for us. If there's something following
          ; us, we generate a cons cell as our resulting generator; otherwise,
          ; just whatever this first element's next gen state is.
          [op (if-let [nxt (next this)]
                (cons gen' (next this))
                gen')]

          ; This generator is exhausted; move on
          (recur (next this) test ctx)))))

  ; Forgettables are transparently unwrapped when treated as generators.
  jepsen.util.Forgettable
  (update [this test ctx event]
    (update @this test ctx event))

  (op [this test ctx]
    (op @this test ctx)))

(defmacro extend-protocol-runtime
  "Extends a protocol to a runtime-defined class. Helpful because some Clojure
  constructs, like promises, use reify rather than classes, and have no
  distinct interface we can extend."
  [proto klass & specs]
  (let [cn (symbol (.getName ^Class (eval klass)))]
    `(extend-protocol ~proto ~cn ~@specs)))

(defonce initialized?
  (atom false))

(defn init!
  "We do some magic to extend the Generator protocol over promises etc, but
  it's fragile and could break with... I think AOT compilation, but also
  apparently plain old dependencies? I'm not certain. It's weird. Just to be
  safe, we move this into a function that gets called by
  jepsen.generator.interpreter, so that we observe the *real* version of the
  promise reify auto-generated class."
  []
  (when (compare-and-set! initialized? false true)
    (eval
      `(extend-protocol-runtime Generator
                                (class (promise))
                                (update [p# test# ctx# event#] p#)

                                (op [p# test# ctx#]
                                    (if (realized? p#)
                                      (op @p# test# ctx#)
                                      [:pending p#]))))))

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
                      (not (history/op? op))
                      (conj "should be either :pending or a jepsen.history.Op")

                      (not (#{:invoke :info :sleep :log} (:type op)))
                      (conj ":type should be :invoke, :info, :sleep, or :log")

                      (not (number? (:time op)))
                      (conj ":time should be a number")

                      (not (:process op))
                      (conj "no :process")

                      (not (->> op :process
                                (context/process->thread ctx)
                                (context/thread-free? ctx)))
                      (conj (str "process " (pr-str (:process op))
                                 " is not free"))))))]
        (when (seq problems)
            (throw+ {:type      ::invalid-op
                     :context   (datafy ctx)
                     :res       res
                     :problems  problems}
                    nil
                    (with-out-str
                      (println "Generator produced an invalid [op, gen'] tuple when asked for an operation:\n")
                      (binding [*print-length* 10]
                        (pprint res))
                      (println "\nSpecifically, this is a problem because:\n")
                      (doseq [p problems]
                        (println " -" p))
                      (println "\nGenerator:\n")
                      (binding [*print-length* 10]
                        (pprint gen))
                      (println "\nContext:\n")
                      (pprint ctx)))))
      [(first res) (Validate. (second res))]))

  (update [this test ctx event]
    (Validate. (update gen test ctx event))))

(defn validate
  "Validates the well-formedness of operations emitted from the underlying
  generator."
  [gen]
  (Validate. gen))

(defrecord FriendlyExceptions [gen]
  Generator
  (op [this test ctx]
    (try
      (when-let [[op gen'] (op gen test ctx)]
        [op (FriendlyExceptions. gen')])
      (catch Throwable t
        (throw+ {:type    ::op-threw
                 :context (datafy ctx)}
                t
                (with-out-str
                  (print "Generator threw" (class t) "-" (.getMessage t) "when asked for an operation. Generator:\n")
                  (binding [*print-length* 10]
                    (pprint gen))
                  (println "\nContext:\n")
                  (pprint (datafy ctx)))))))

  (update [this test ctx event]
    (try
      (when-let [gen' (update gen test ctx event)]
        (FriendlyExceptions. gen'))
      (catch Throwable t
        (throw+ {:type    ::update-threw
                 :context (datafy ctx)
                 :event   event}
                t
                  (with-out-str
                    (print "Generator threw " t " when updated with an event. Generator:\n")
                    (binding [*print-length* 10]
                      (pprint gen))
                    (println "\nContext:\n")
                    (pprint (datafy ctx))
                    (println "Event:\n")
                    (pprint event)))))))

(defn friendly-exceptions
  "Wraps a generator, so that exceptions thrown from op and update are wrapped
  with a :type ::op-threw or ::update-threw Slingshot exception map, including
  the generator, context, and event which caused the exception."
  [gen]
  (FriendlyExceptions. gen))

(defrecord Trace [k gen]
  Generator
  (op [_ test ctx]
    (binding [*print-length* 8]
      (try (let [[op gen'] (op gen test ctx)]
             (info k :op (with-out-str
                           (println "\nContext:" ctx)
                           (println "Operation:" op)
                           (println "Generator:")
                           (pprint gen)))
             (when op
               [op (when gen' (Trace. k gen'))]))
           (catch Throwable t
             (info k :op :threw
                   (with-out-str
                     (println "\nContext:" ctx)
                     (println "Operation:" op)
                     (println "Generator:")
                     (pprint gen)))
             (throw t)))))

  (update [_ test ctx event]
    (binding [*print-length* 8]
      (try (let [gen' (update gen test ctx event)]
             (info k :update (with-out-str
                               (println "\nContext:" ctx)
                               (println "Event:" event)
                               (println "Generator:")
                               (pprint gen)))
             (when gen' (Trace. k gen')))
           (catch Throwable t
             (info k :update :threw (with-out-str
                                      (println "\nContext:" ctx)
                                      (println "Event:" event)
                                      (println "Generator:")
                                      (pprint gen)))
             (throw t))))))

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

(defn concat
  "Where your generators are sequences, you can use Clojure's `concat` to make
  them a generator. This `concat` is useful when you're trying to concatenate
  arbitrary generators. Right now, (concat a b c) is simply '(a b c)."
  [& gens]
  (seq gens))

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

(defrecord OnUpdate [f gen]
  Generator
  (op [this test ctx]
    (when-let [[op gen'] (op gen test ctx)]
      [op (OnUpdate. f gen')]))

  (update [this test ctx event]
    (f this test ctx event)))

(defn on-update
  "Wraps a generator with an update handler function. When an update occurs,
  calls (f this test ctx event), and returns whatever f does--presumably, a new
  generator. Can also be helpful for side effects--for instance, to update some
  shared mutable state when an update occurs."
  [f gen]
  (OnUpdate. f gen))

(defn on-threads-context
  "For backwards compatibility; filters a context to just threads matching (f
  thread). Use context/make-thread-filter for performance."
  [f context]
  ((context/make-thread-filter f context) context))

(defrecord OnThreads [f context-filter gen]
  Generator
  (op [this test ctx]
    (when-let [[op gen'] (op gen test (context-filter ctx))]
      [op (OnThreads. f context-filter gen')]))

  (update [this test ctx event]
    (if (f (process->thread ctx (:process event)))
      (OnThreads. f context-filter (update gen test (context-filter ctx) event))
      this)))

(defn on-threads
  "Wraps a generator, restricting threads which can use it to only those
  threads which satisfy (f thread). Alters the context passed to the underlying
  generator: it will only include free threads and workers satisfying f.
  Updates are passed on only when the thread performing the update matches f."
  [f gen]
  (OnThreads. f (context/make-thread-filter f) gen))

(def on "For backwards compatibility" on-threads)

(defn soonest-op-map
  "Takes a pair of maps wrapping operations. Each map has the following
  structure:

    :op       An operation
    :weight   An optional integer weighting.

  Returns whichever map has an operation which occurs sooner. If one map is
  nil, the other happens sooner. If one map's op is :pending, the other happens
  sooner. If one op has a lower :time, it happens sooner. If the two ops have
  equal :times, resolves the tie randomly proportional to the two maps'
  respective :weights. With weights 2 and 3, returns the first map 2/5 of the
  time, and the second 3/5 of the time.

  The :weight of the returned map is the *sum* of both weights if their times
  are equal, which makes this function suitable for use in a reduction over
  many generators.

  Why is this nondeterministic? Because we use this function to decide between
  several alternative generators, and always biasing towards an earlier or
  later generator could lead to starving some threads or generators."
  ([] nil)
  ([m] m)
  ([m1 m2]
   (condp identical? nil
     m1 m2
     m2 m1
     (let [op1 (:op m1)
           op2 (:op m2)]
       (condp identical? :pending
         op1 m2
         op2 m1
         (let [t1 (:time op1)
               t2 (:time op2)]
           (if (= t1 t2)
             ; We have a tie; decide based on weights.
             (let [w1 (:weight m1 1)
                   w2 (:weight m2 1)
                   w  (+ w1 w2)]
               (assoc (if (< (rand-int w) w1) m1 m2)
                      :weight w))
             ; Not equal times; which comes sooner?
             (if (< t1 t2)
               m1
               m2))))))))

(defrecord Any [gens]
  Generator
  (op [this test ctx]
    (when-let [{:keys [op gen' i]}
               (->> gens
                    (map-indexed
                      (fn [i gen]
                        (when-let [[op gen'] (op gen test ctx)]
                          {:op    op
                           :gen'  gen'
                           :i     i})))
                    (reduce soonest-op-map nil))]
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

(defn each-thread-ensure-context-filters!
  "Ensures an EachThread has context filters for each thread."
  [context-filters ctx]
  (when-not (realized? context-filters)
    (deliver context-filters
             (reduce (fn compute-context-filters [cfs thread]
                       (assoc cfs thread (context/make-thread-filter
                                           #{thread}
                                           ctx)))
                     {}
                     (context/all-threads ctx)))))

(defrecord EachThread [fresh-gen context-filters gens]
  ; fresh-gen is a generator we use to initialize a thread's state, the first
  ; time we see it.
  ; context-filters is a promise of a map of threads to context filters; lazily
  ; initialized.
  ; gens is a map of threads to generators.
  Generator
  (op [this test ctx]
    (each-thread-ensure-context-filters! context-filters ctx)
    (let [{:keys [op gen' thread] :as soonest}
          (->> (context/free-threads ctx)
               (keep (fn [thread]
                       (let [gen     (get gens thread fresh-gen)
                             ; Give this generator a context *just* for one
                             ; thread
                             ctx     ((@context-filters thread) ctx)]
                         (when-let [[op gen'] (op gen test ctx)]
                           {:op      op
                            :gen'    gen'
                            :thread  thread}))))
               (reduce soonest-op-map nil))]
      (cond ; A free thread has an operation
            soonest [op (EachThread. fresh-gen context-filters
                                     (assoc gens thread gen'))]

            ; Some thread is busy; we can't tell what to do just yet
            (not= (context/free-thread-count ctx)
                  (context/all-thread-count ctx))
            [:pending this]

            ; Every thread is exhausted
            true
            nil)))

  (update [this test ctx event]
    (each-thread-ensure-context-filters! context-filters ctx)
    (let [process (:process event)
          thread (process->thread ctx process)
          gen    (get gens thread fresh-gen)
          ctx    ((@context-filters thread) ctx)
          gen'   (update gen test ctx event)]
      (EachThread. fresh-gen context-filters (assoc gens thread gen')))))

(defn each-thread
  "Takes a generator. Constructs a generator which maintains independent copies
  of that generator for every thread. Each generator sees exactly one thread in
  its free process list. Updates are propagated to the generator for the thread
  which emitted the operation."
  [gen]
  (EachThread. gen (promise) {}))

(defrecord Reserve [ranges all-ranges context-filters gens]
  ; ranges is a collection of sets of threads engaged in each generator.
  ; all-ranges is the union of all ranges.
  ; context-filters is a vector of context filtering functions, one for each
  ; range (and the default gen last).
  ; gens is a vector of generators corresponding to ranges, followed by the
  ; default generator.
  Generator
  (op [_ test ctx]
    (let [; A transducer to compute op/gen'/weight/i maps for each of `ranges`
          xf (map-indexed
               (fn per-range [i threads]
                 (let [gen (nth gens i)
                       ; Restrict context to this range of threads
                       ctx ((nth context-filters i) ctx)]
                   ; Ask this range's generator for an op
                   (when-let [[op gen'] (op gen test ctx)]
                     ; Remember our index
                     {:op     op
                      :gen'   gen'
                      :weight (count threads)
                      :i      i}))))
          ; And for the default generator...
          default-op-map
          (let [ctx ((peek context-filters) ctx)]
            ; And construct a triple for the default generator
            (when-let [[op gen'] (op (peek gens) test ctx)]
              (assert ctx)
              {:op     op
               :gen'   gen'
               :weight (context/all-thread-count ctx)
               :i      (count ranges)}))
          ; Find soonest generator
          {:keys [op gen' i] :as soonest}
          (transduce xf soonest-op-map default-op-map ranges)]
      (when soonest
        ; A range has an operation to do!
        [op (Reserve. ranges all-ranges context-filters (assoc gens i gen'))])))

  (update [this test ctx event]
    (let [process (:process event)
          thread  (process->thread ctx process)
          ; Find generator whose thread produced this event.
          i (reduce (fn red [i range-]
                      (if (range- thread)
                        (reduced i)
                        (inc i)))
                    0
                    ranges)]
      (Reserve. ranges all-ranges context-filters
                (c/update gens i update test ctx event)))))

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
                  ; inclusive, upper exclusive. TODO: I think there might be a
                  ; bug here: if we construct nested reserves or otherwise
                  ; restrict threads, an inner reserve might not understand
                  ; that its threads don't start at 0.
                  (reduce (fn [[n gens] [thread-count gen]]
                            (let [n' (+ n thread-count)]
                              [n' (conj gens [(set (range n n')) gen])]))
                          [0 []])
                  second)
        ranges      (mapv first gens)
        all-ranges  (reduce set/union ranges)
        ; Compute context filters for all ranges
        context-filters (mapv context/make-thread-filter
                              (c/concat ranges
                                      [(complement all-ranges)]))
        gens        (mapv second gens)
        default     (last args)
        gens        (conj gens default)]
    (assert default)
    (Reserve. ranges all-ranges context-filters gens)))

(declare nemesis)

(defn clients
  "In the single-arity form, wraps a generator such that only clients
  request operations from it. In its two-arity form, combines a generator of
  client operations and a generator for nemesis operations into one. When the
  process requesting an operation is :nemesis, routes to the nemesis generator;
  otherwise to the client generator."
  ([client-gen]
   (on (context/all-but :nemesis) client-gen))
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
    (when-not (= 0 (count gens))
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
  mixture of operations.

  TODO: This can interact badly with generators that return :pending; gen/mix
  won't let other generators (which could help us get unstuck!) advance. We
  should probably cycle on :pending."
  [gens]
  (when (seq gens)
    (Mix. (rand-int (count gens)) (vec gens))))

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
  nil. Occurs only once; use `repeat` to repeat."
  [msg]
  {:type :log, :value msg})

(defrecord Repeat [^long remaining gen]
  ; Remaining is positive for a limit, or -1 for infinite repeats.
  Generator
  (op [_ test ctx]
    (when-not (= 0 remaining)
      (when-let [[op gen'] (op gen test ctx)]
        [op (Repeat. (max -1 (dec remaining)) gen)])))

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

(defrecord Cycle [remaining original-gen gen]
  Generator
  (op [_ test ctx]
    (when-not (zero? remaining)
      (if-let [[op gen'] (op gen test ctx)]
        ; We've got operations still. Emit them and let the generator evolve.
        [op (Cycle. remaining original-gen gen')]
        ; Out of operations. Recycle! If you actually hit MIN_INT doing this...
        ; you probably have bigger problems on your hands.
        (op (Cycle. (dec remaining) original-gen original-gen)
            test
            ctx))))

  (update [this test ctx event]
    (Cycle. remaining original-gen (update gen test ctx event))))

(defn cycle
  "Wraps a finite generator so that once it completes (e.g. emits nil), it
  begins again. With an optional integer limit, repeats the generator that many
  times. When the generator returns nil, it is reset to its original value and
  the cycle repeats. Updates are propagated to the current generator, but do
  not affect the original. Not sure if this is the right call--might change
  that later."
  ([gen]
   (Cycle. -1 gen gen))
  ([limit gen]
   (Cycle. limit gen gen)))

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
          (when-not (:time op) (warn "No time for op:" op))
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

(defrecord Stagger [dt next-time gen]
  Generator
  (op [this test ctx]
    (when-let [[op gen'] (op gen test ctx)]
      (let [now       (:time ctx)
            next-time (or next-time now)]
        (cond ; No need to do anything to pending ops
              (= :pending op)
              [op this]

              ; We're ready to issue this operation.
              (<= next-time (:time op))
              [op (Stagger. dt (+ (:time op) (long (rand dt))) gen')]

              ; Not ready yet
              true
              [(assoc op :time next-time)
               (Stagger. dt (+ next-time (long (rand dt))) gen')]))))

  (update [_ test ctx event]
    (Stagger. dt next-time (update gen test ctx event))))

(defn stagger
  "Wraps a generator. Operations from that generator are scheduled at uniformly
  random intervals between 0 to 2 * (dt seconds).

  Unlike Jepsen's original version of `stagger`, this actually *means*
  'schedule at roughly every dt seconds', rather than 'introduce roughly dt
  seconds of latency between ops', which makes this less sensitive to request
  latency variations.

  Also note that unlike Jepsen's original version of `stagger`, this delay
  applies to *all* operations, not to each thread independently. If your old
  stagger dt is 10, and your concurrency is 5, your new stagger dt should be
  2."
  [dt gen]
  (let [dt (long (util/secs->nanos (* 2 dt)))]
    (Stagger. dt nil gen)))

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

; dt is our interval between ops in nanos
; next-time is the next timestamp we plan to emit.
(defrecord Delay [dt next-time gen]
  Generator
  (op [_ test ctx]
    (when-let [[op gen'] (op gen test ctx)]
      (if (= op :pending)
        ; Just pass these through; we don't know when they'll occur!
        [op (Delay. dt next-time gen')]

        ; OK we have an actual op. Compute its new event time.
        (let [next-time (or next-time (:time op))
              op        (c/update op :time max next-time)]
          [op (Delay. dt (+ (:time op) dt) gen')]))))

  (update [this test ctx event]
    (Delay. dt next-time (update gen test ctx event))))

(defn delay
  "Given a time dt in seconds, and an underlying generator gen, constructs a
  generator which tries to emit operations exactly dt seconds apart. Emits
  operations more frequently if it falls behind. Like `stagger`, this should
  result in histories where operations happen roughly every dt seconds.

  Note that this definition of delay differs from its stateful cousin delay,
  which a.) introduced dt seconds of delay between *completion* and subsequent
  invocation, and b.) emitted 1/dt ops/sec *per thread*, rather than globally."
  [dt gen]
  (Delay. (long (util/secs->nanos dt)) nil gen))

(defn sleep
  "Emits exactly one special operation which causes its receiving process to do
  nothing for dt seconds. Use (repeat (sleep 10)) to sleep repeatedly."
  [dt]
  {:type :sleep, :value dt})

(defrecord Synchronize [gen]
  Generator
  (op [this test ctx]
    (if (= (context/free-thread-count ctx)
           (context/all-thread-count ctx))
      ; We're ready, replace ourselves with the generator
      (op gen test ctx)
      ; Not yet
      [:pending this]))

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

(defrecord UntilOk [gen done? active-processes]
  Generator
  (op [this test ctx]
    (when-not done?
      (when-let [[op gen'] (op gen test ctx)]
        (if (= :pending op)
          [op (assoc this :gen gen')]
          [op (UntilOk. gen' done? (conj active-processes (:process op)))]))))

  (update [this test ctx event]
    (let [gen' (update gen test ctx event)
          p    (:process event)]
      (if (contains? active-processes p)
        ; This is an update related to one of our operations.
        (case (:type event)
          ; We're finished; no need to update any more!
          :ok (UntilOk. gen' true (disj active-processes p))

          ; Crashed or failed; process no longer active, but we're not done.
          (:info, :fail) (UntilOk. gen' done? (disj active-processes p))

          ; Pass through
          (UntilOk. gen' done? active-processes))
        ; Some unrelated update
        (UntilOk. gen' done? active-processes)))))

(defn until-ok
  "Wraps a generator, yielding operations from it until one of those operations
  completes with :type :ok."
  [gen]
  (UntilOk. gen false #{}))

(defrecord FlipFlop [gens i]
  Generator
  (op [this test ctx]
    (when-let [[op gen'] (op (nth gens i) test ctx)]
      [op (FlipFlop. (assoc gens i gen')
                     (mod (inc i) (count gens)))]))

  (update [this test ctx event]
    this))

(defn flip-flop
  "Emits an operation from generator A, then B, then A again, then B again,
  etc. Stops as soon as any gen is exhausted. Updates are ignored."
  [a b]
  (FlipFlop. [a b] 0))

(defrecord CycleTimes [period    ; Total period of the cycle, in nanos
                       t0        ; Starting time, in nanos (initially nil)
                       intervals ; Vector of durations in nanos that each
                                 ; generator should last for.
                       cutoffs   ; Vector of times in nanos below which that
                                 ; particular generator starts. Omits the last
                                 ; generator.
                       gens]     ; Vector of generators
  Generator
  (op [this test ctx]
    ; We start by figuring out the current generator based on the context time.
    (let [now       (:time ctx)
          ; When's the zero point of our timeline?
          t0        (or t0 (:time ctx))
          ; How far are we into the current cycle?
          in-period (mod (- now t0) period)
          ; When was the start of the current cycle?
          cycle-start (- now in-period)
          ; What generator do we think is most likely to have an operation for
          ; us?
          i         (loop [i 0]
                      (if (or (= i (count cutoffs))
                              (< in-period (nth cutoffs i)))
                        i
                        (recur (inc i))))]
      ; Now we cycle through the generators in order, looking for one which can
      ; produce an operation which falls before the end of its window.
      (loop [i i
             ; When we ask a later generator for an operation, we don't want to
             ; ask it for an operation with the *current* time, because that
             ; generator won't actually be valid until *later*. Instead we're
             ; going to pretend its time t had arrived already, and see what
             ; the answer *would* be at that time.
             t (reduce + cycle-start (take i intervals))]
        (let [gen      (nth gens i)
              interval (nth intervals i)
              ; When does the next generator start?
              t'       (+ t interval)
              ; Ask this generator for an operation. We use the current time,
              ; or the start of the generator's window, whichever is higher.
              [op gen'] (op gen test (assoc ctx :time (max now t)))]
          (cond ; Ah, this generator is exhausted. So are we.
                (nil? op)
                nil

                ; We're pending--we might choose to emit an operation before
                ; this window is over.
                (= :pending op)
                [:pending (CycleTimes. period t0 intervals cutoffs
                                       (assoc gens i gen'))]

                ; This operation falls before the generator's window ends.
                (< (:time op) t')
                [op (CycleTimes. period t0 intervals cutoffs
                                 (assoc gens i gen'))]

                ; This operation falls after the generator's window ends; try
                ; the next generator.
                true
                (recur (mod (inc i) (count gens))
                       t'))))))

  (update [this test ctx event]
    (CycleTimes. period t0 intervals cutoffs
                 (mapv (fn updater [gen] (update gen test ctx event)) gens))))

(defn cycle-times
  "Cycles between several generators on a rotating schedule. Takes a flat
  series of [time, generator] pairs, like so:

      (cycle-times 5  {:f :write}
                   10 (gen/stagger 1 {:f :read}))

  This generator emits writes for five seconds, then staggered reads for ten
  seconds, then goes back to writes, and so on. Generator state is preserved
  from cycle to cycle, which makes this suitable for e.g. interleaving quiet
  periods into a nemesis generator which needs to perform a specific sequence
  of operations like :add-node, :remove-node, :add-node ...

  Updates are propagated to all generators."
  [& specs]
  (when (seq specs)
    (assert (even? (count specs)))
    (let [intervals       (->> specs
                               (partition 1 2)
                               (mapcat identity)
                               (mapv (comp long util/secs->nanos)))
          gens            (->> specs next (partition 1 2) vec)
          period          (reduce + intervals)
          cutoffs         (vec (reductions + intervals))]
      (CycleTimes. period nil intervals cutoffs gens))))
