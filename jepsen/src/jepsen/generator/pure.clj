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
  ready invocation, we apply the invocation as an input to the graph, obtaining
  a new graph, and hand the operation to the relevant client.

  ## Contexts

  A *context* is a map which provides context for generators. For instance, a
  generator might need to know the number of threads which will ask it for
  operations. It can get that number from the *context*. Users can add their
  own values to the context map, which allows two generators to share state.
  When one generator calls another, it can pass a modified version of the
  context, which allows us to write generators that, say, run two independent
  workloads, each with their own concurrency and thread mappings.

  The standard context mappings, which are provided by Jepsen when invoking the
  top-level generator, and can be expected by every generator, are:

      :time           The current Jepsen linear time, in nanoseconds
      :free-processes A collection of idle processes which could perform work
      :workers        A map of thread identifiers to process identifiers

  ## Fetching an operation

  We use `(op gen test context)` to ask the generator for the next invocation
  that we can process.

  The operation can have three forms:

  - The generator may return `nil`, which means the generator is done, and
    there is nothing more to do.
  - The generator may return :pending, which means there might be more
    ops later
  - The generator may return an operation, in which case:
    - If its time is in the past, we can evaluate it now
    - If its time is in the future, we wait until either:
      - The time arrives
      - Circumstances change (e.g. we update the generator)

  But (op gen test context) returns more than just an operation; it also
  returns the *subsequent state* of the generator, if that operation were to be
  emitted. The two are bundled into a tuple.

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
  (:refer-clojure :exclude [concat delay update])
  (:require [clojure.core :as c]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [util :as util]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defprotocol Generator
  (update [gen test context event]
          "Updates the generator to reflect an event having taken place")

  (op [gen test context]))

(extend-protocol Generator
  nil
  (update [gen test ctx event] nil)
  (op [this test ctx] nil)

  clojure.lang.IPersistentMap
  (update [this test ctx event] this)
  (op [this test ctx]
    [(if-let [p (first (:free-processes ctx))]
       ; Automatically assign type, time, and process from the context, if not
       ; provided.
       (cond-> this
         (nil? (:time this))     (assoc :time (:time ctx))
         (nil? (:process this))  (assoc :process p)
         (nil? (:type this))     (assoc :type :invoke))

       ; No process free to accept our request
       :pending)
     this])

  clojure.lang.AFunction
  (update [f test ctx event] f)
  (op [f test ctx]
    (when-let [x (try (f test ctx)
                      (catch clojure.lang.ArityException e
                        (f)))]
      (condp instance? x
        ; Ask the map to generate an operation for us.
        clojure.lang.IPersistentMap     [(first (op x test ctx)) f]
        ; Return the (presumably a pair) directly
        clojure.lang.IPersistentVector  x
        ; ???
        (throw+ {:type :unexpected-return
                 :value x})))))

;; Helpers

(defn random-int-seq
  "Generates a reproducible sequence of random longs, given a random seed. If
  seed is not provided, taken from (rand-int))."
  ([] (random-int-seq (rand-int Integer/MAX_VALUE)))
  ([seed]
   (let [gen (java.util.Random. seed)]
     (repeatedly #(.nextLong gen)))))

;; Generators!

(defrecord Validate [gen]
  Generator
  (op [_ test ctx]
    (when-let [[op gen'] (op gen test ctx)]
      (let [problems (if (= :pending op)
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

                         (not-any? #{(:process op)} (:free-processes ctx))
                         (conj (str "process " (:process op) " is not free"))))]
        (when (seq problems)
          (throw+ {:type      :invalid-op
                   :generator gen
                   :context   ctx
                   :op        op
                   :problems  problems})))
      [op (Validate. gen')]))

  (update [this test ctx event]
    (Validate. (update gen test ctx event))))

(defn validate
  "Validates the well-formedness of operations emitted from the underlying
  generator."
  [gen]
  (Validate. gen))


(defrecord Limit [remaining gen]
  Generator
  (op [_ test ctx]
    (when (pos? remaining)
      (let [[op gen'] (op gen test ctx)]
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


(defrecord TimeLimit [limit cutoff gen]
  Generator
  (op [_ test ctx]
    (let [[op gen'] (op gen test ctx)
          cutoff    (or cutoff (+ (:time op) limit))]
      (when (< (:time op) cutoff)
        [op (TimeLimit. limit cutoff gen')])))

  (update [this test ctx event]
    (TimeLimit. limit cutoff (update gen test ctx event))))

(defn time-limit
  "Takes a time in seconds, and an underlying generator. Once this emits an
  operation (taken from that underlying generator), will only emit operations
  for dt seconds."
  [dt gen]
  (TimeLimit. (long (util/secs->nanos dt)) nil gen))


(defrecord DelayTil [dt anchor wait-til gen]
  Generator
  (op [_ test ctx]
    (let [[op gen']  (op gen test ctx)
          ; The next op should occur at time t
          t         (if wait-til
                      (max wait-til (:time op))
                      (:time op))
          ; Update op
          op'       (assoc op :time t)
          ; Initialize our anchor if we don't have one
          anchor'   (or anchor t)
          ; And compute the next wait-til time after t
          wait-til' (+ t (- dt (mod (- t anchor') dt)))
          ; Our next generator state
          gen'      (DelayTil. dt anchor' wait-til' gen')]
      [op' gen']))

  (update [this test ctx event]
    (DelayTil. dt anchor wait-til (update gen test ctx event))))

(defn delay-til
  "Given a time dt in seconds, and an underlying generator gen, constructs a
  generator which emits operations such that successive invocations are at
  least dt seconds apart."
  [dt gen]
  (DelayTil. (long (util/secs->nanos dt)) nil nil gen))
