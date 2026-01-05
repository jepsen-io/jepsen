(ns jepsen.antithesis.composer
  "Antithesis' Test Composer drives Jepsen by calling shell scripts, which
  communicate with the test via a control directory full of FIFOs. Each FIFO
  represents a single test composer action. They work like this:

  1. Test composer calls a script to do something.
  2. Script creates a fifo like /run/jepsen/op-1234, and blocks on it.
  3. Jepsen, watching /run/jepsen, detects the new fifo
  4. Jepsen fires off an invocation
  5. The operation completes
  6. Jepsen writes the results of the operation to /run/jepsen/invoke_1234
  7. Jepsen closes the FIFO
  8. The shell script prints the results out and exits
  9. The test composer receives the results

  The types of commands are:

  - `op-123`: Runs a single operation from the generator
  - `check`: Wraps up the test's :generator, runs :final-generator, and
             checks the history.

  This namespace provides the Jepsen-side infrastructure for watching the FIFO
  directory, a test runner like `jepsen.core/run!`, and a CLI command called
  `antithesis` which works like `test`, but uses our runner instead."
  (:require [bifurcan-clj [core :as b]
                          [map :as bm]]
            [clj-commons.slingshot :refer [try+ throw+]]
            [clojure [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn fatal]]
            [dom-top.core :refer []]
            [fipp.edn :refer [pprint]]
            [jepsen [core :as jepsen]
                    [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [store :as store]
                    [util :as util :refer [with-thread-name
                                           relative-time-nanos
                                           real-pmap
                                           fcatch]]]
            [jepsen.generator [interpreter :as gi]
                              [context :as gc]]
            [jepsen.store.format :as store.format]
            [potemkin :refer [definterface+]])
  (:import (java.util.concurrent ArrayBlockingQueue
                                 BlockingQueue
                                 TimeUnit)
           (io.lacuna.bifurcan ISet
                               Set)
           (java.nio.file FileSystems
                          Path
                          StandardWatchEventKinds
                          WatchEvent
                          WatchEvent$Kind
                          WatchService)))

(def fifo-dir
  "The directory where we watch for fifo operations."
  "/tmp/jepsen/antithesis")

(definterface+ IFifoOp
  (complete-fifo-op! [this ^long status out]
            "Completes this operation with the given exit status (ignored) and
            stdout, which is printed to the FIFO."))

(defrecord FifoOp [name ^Path path]
  IFifoOp
  (complete-fifo-op! [this status out]
    (info "Completing" path)
    (spit (.toFile path) out)))

(defn ^BlockingQueue fifo-queue
  "Constructs a queue full of pending FIFO operations. The fifo watcher writes
  to this queue, and the interpreter reads from it, completing its promises."
  []
  ; Fine, whatever.
  (ArrayBlockingQueue. 64))

(defn ^WatchService fifo-watcher!
  "Takes a test, and launches a future which watches the FIFO directory for new
  files. Each new file results in a FifoOp delivered to `(:fifo-queue test)`.
  Creates the directory if it does not exist. Always empties directory before
  watching."
  [test]
  ; Ensure FIFO dir exists
  (.mkdirs (io/file fifo-dir))
  ; Clear it out
  (->> fifo-dir io/file file-seq next (mapv io/delete-file))

  (let [fs            (FileSystems/getDefault)
        fifo-dir-path (.getPath fs fifo-dir (make-array String 0))
        ^BlockingQueue queue (:fifo-queue test)]
    (future
      (with-thread-name "Jepsen FIFO watcher"
        (with-open [watch-service (.newWatchService fs)]
          (let [kinds         (into-array [StandardWatchEventKinds/ENTRY_CREATE])
                watch-key     (.register fifo-dir-path watch-service kinds)]
            (info "Waiting for FIFOs in" fifo-dir)
            (loop []
              (info "Waiting on watch-service")
              (let [key (.take watch-service)]
                (doseq [event (.pollEvents key)]
                  (info :fifo-event event (.kind event))
                  (condp = (.kind event)
                    StandardWatchEventKinds/OVERFLOW
                    (do (fatal "Couldn't keep up with FIFOs!")
                        (System/exit 1))

                    StandardWatchEventKinds/ENTRY_CREATE
                    (let [^Path path (.context ^WatchEvent event)
                          ; This seems to come out as just a relative path, but
                          ; ???
                          short-name (str (.getName path
                                                    (dec (.getNameCount path))))
                          full-path  (.resolve fifo-dir-path path)
                          op (FifoOp. short-name full-path)]
                      (info :fifo-enqueue op)
                      (.put queue op))))
                (if (.reset key)
                  (recur)
                  ; No longer registered
                  :no-longer-registered)))))))))

(defmacro with-fifo-watcher!
  "Opens a fifo watcher for the duration of the body, and terminates it when
  the body completes."
  [test & body]
  `(let [watcher# (fifo-watcher! ~test)]
     (try ~@body
       (finally (future-cancel watcher#)))))

;; Generator support.

(defrecord MainGen [gen]
  gen/Generator
  (op [this test ctx]
    (when (identical? :main @(:antithesis-phase test))
      (when-let [[op gen'] (gen/op gen test ctx)]
        [op (MainGen. gen')])))

  (update [this test ctx event]
    (MainGen. (gen/update gen test ctx event))))

(defn main-gen
  "We need a way to terminate the regular generator and move to the final
  generator. However, the regular generator and final generator may be
  *wrapped* in some kind of state-tracking generator which allows context to
  pass from one to the other--and we don't have a way to reach inside that
  single generator and trigger a flip.

  This generator is intended to be called by the test author, and wraps the
  main phase generator. It uses an atom, stored in the test: :antithesis-phase.
  This generator emits operations so long as the phase `:main`. Otherwise it
  emits `nil`, which forces any composed generator to proceed to the final
  phase. Once all final operations are done, the atom flips to `:done`."
  [gen]
  (MainGen. gen))

;; Interpreter. This is adapted from jepsen.generator.interpreter, but uses
;; different control flow at the top level, because we're driven by the
;; *external* FIFO watcher, not by the generator directly.

(defn interpret!
  "Borrowed from jepsen.generator.interpreter, with adaptations for our FIFO
  queue. Notably, the test now contains a :fifo-queue, which delivers
  InterpreterMsgs to the interpreter thread. These messages are of two types:

    :op     Perform a single operation from the (presumably main phase) of the
            generator. Delivers the [invoke, complete] pair to the message's
            promise when done.

    :check  Enters the final phase of the generator. Flips test-phase to
            :final, and consumes generator operations as quickly as possible
            to perform the final phase. When these operations are exhausted,
            the interpreter returns, triggering the final analysis.

  Takes a test with a :store :handle open. Causes the test's reference to the
  :generator to be forgotten, to avoid retaining the head of infinite seqs.
  Opens a writer for the test's history using that handle. Creates an initial
  context from test and evaluates all ops from (:gen test). Spawns a thread for
  each worker, and hands those workers operations from gen; each thread applies
  the operation using (:client test) or (:nemesis test), as appropriate.
  Invocations and completions are journaled to a history on disk. Returns a new
  test with no :generator and a completed :history.

  Generators are automatically wrapped in friendly-exception and validate.
  Clients are wrapped in a validator as well.

  Automatically initializes the generator system, which, on first invocation,
  extends the Generator protocol over some dynamic classes like (promise)."
  [test]
  (gen/init!)
  (with-open [history-writer (store.format/test-history-writer!
                               (:handle (:store test))
                               test)]
    (let [ctx         (gen/context test)
          worker-ids  (gen/all-threads ctx)
          ^BlockingQueue fifo-queue (:fifo-queue test)
          _ (assert (instance? BlockingQueue fifo-queue))
          completions (ArrayBlockingQueue.
                        (.size ^ISet worker-ids))
          workers     (mapv (partial gi/spawn-worker test completions
                                     (gi/client-nemesis-worker))
                            worker-ids)
          invocations (into {} (map (juxt :id :in) workers))
          gen         (->> (:generator test)
                           deref
                           gen/friendly-exceptions
                           gen/validate)
          phase (:antithesis-phase test)
          ; Forget generator
          _           (util/forget! (:generator test))
          test        (dissoc test :generator)]
      ; HERE
      (try+
        (loop [ctx            ctx
               gen            gen
               op-index       0     ; Index of the next op in the history
               outstanding    0     ; Number of in-flight ops
               ; How long to poll on the completion queue, in micros.
               poll-timeout   0
               ; The FifoOp each thread is processing
               fifo-ops    (b/linear bm/empty)]
          ; First, can we complete an operation? We want to get to these first
          ; because they're latency sensitive--if we wait, we introduce false
          ; concurrency.
          (if-let [op' (.poll completions poll-timeout TimeUnit/MICROSECONDS)]
            (let [;_      (prn :completed op')
                  thread (gen/process->thread ctx (:process op'))
                  time    (util/relative-time-nanos)
                  ; Update op with index and new timestamp
                  op'     (assoc op' :index op-index :time time)
                  ; Update context with new time and thread being free
                  ctx     (gc/free-thread ctx time thread)
                  ; Let generator know about our completion. We use the context
                  ; with the new time and thread free, but *don't* assign a new
                  ; process here, so that thread->process recovers the right
                  ; value for this event.
                  gen     (gen/update gen test ctx op')
                  ; Threads that crash (other than the nemesis), or which
                  ; explicitly request a new process, should be assigned new
                  ; process identifiers.
                  ctx     (if (and (not= :nemesis thread)
                                   (or (= :info (:type op'))
                                       (:end-process? op')))
                            (gc/with-next-process ctx thread)
                            ctx)
                  ; The FifoOp to complete
                  fifo-op  (bm/get fifo-ops thread nil)
                  fifo-ops (bm/remove fifo-ops thread)]
              (info :op' op' :fifo-op fifo-op)
              ; Log completion and move on
              (if (gi/goes-in-history? op')
                (do (store.format/append-to-big-vector-block!
                      history-writer op')
                    (when fifo-op (complete-fifo-op! fifo-op 0 (pr-str op')))
                    (recur ctx gen (inc op-index) (dec outstanding) 0 fifo-ops))
                (do (when fifo-op (complete-fifo-op! fifo-op 0 (pr-str op')))
                    (recur ctx gen op-index (dec outstanding) 0 fifo-ops))))

            ; There's nothing to complete; let's see what the generator's up to
            (let [time        (util/relative-time-nanos)
                  ctx         (assoc ctx :time time)
                  [op gen']   (gen/op gen test ctx)
                  fifo-op     (.peek fifo-queue)]
              (when fifo-op (info :fifo-op fifo-op))
              (cond
                ; As a special case, the fifo op "check" flips our phase.
                (and fifo-op
                     (= "check" (:name fifo-op)))
                ; Gosh this is a gross hack. We're just going to shove the fifo
                ; op into the phase, so it can be completed when the checker is
                ; done.
                (do (info "Antithesis requested a check; main phase ending...")
                    (reset! phase fifo-op)
                    (.take fifo-queue)
                    (recur ctx gen op-index outstanding poll-timeout fifo-ops))

                ; We're exhausted, but workers might still be going.
                (nil? op)
                (if (pos? outstanding)
                  ; Still waiting on workers
                  (recur ctx gen op-index outstanding
                         (long gi/max-pending-interval) fifo-ops)
                  ; Good, we're done. Tell workers to exit...
                  (do (doseq [[thread queue] invocations]
                        (.put ^ArrayBlockingQueue queue {:type :exit}))
                      ; Wait for exit
                      (dorun (map (comp deref :future) workers))
                      ; Await completion of writes
                      (.close history-writer)
                      ; And return history
                      (let [history-block-id (:block-id history-writer)
                            history
                            (-> (:handle (:store test))
                                (store.format/read-block-by-id
                                  history-block-id)
                                :data
                                (h/history {:dense-indices? true
                                            :have-indices? true
                                            :already-ops? true}))]
                        (assoc test :history history))))

                ; Nothing we can do right now. Let's try to complete something.
                (identical? :pending op)
                (recur ctx gen op-index
                       outstanding (long gi/max-pending-interval) fifo-ops)

                ; Good, we've got an invocation. Do we need to wait to run it?
                true
                (if (or ; We can't run operations from the future
                        (< time (:time op))
                        ; In the main phase, we can't run until a fifo op
                        ; arrives
                        (and (identical? :main @phase)
                             (nil? fifo-op)))

                  ; Can't evaluate this op yet!
                  (recur ctx gen op-index outstanding
                         ; Unless something changes, we don't need to ask
                         ; the generator for another op until it's time.
                         (long (/ (- (:time op) time) 1000))
                         fifo-ops)

                  ; Good, we can run this.
                  (let [thread (gen/process->thread ctx (:process op))
                        op (assoc op :index op-index)
                        ; Log the invocation
                        goes-in-history? (gi/goes-in-history? op)
                        _ (when goes-in-history?
                            (store.format/append-to-big-vector-block!
                              history-writer op))
                        op-index' (if goes-in-history? (inc op-index) op-index)
                        ; Dispatch it to a worker
                        _ (.put ^ArrayBlockingQueue (get invocations thread) op)
                        ; Update our context to reflect
                        ctx (gc/busy-thread ctx
                                            (:time op) ; Use time instead?
                                            thread)
                        ; Let the generator know about the invocation
                        gen' (gen/update gen' test ctx op)
                        ; Remember that this thread is servicing this fifo op
                        _         (when fifo-op (.take fifo-queue))
                        fifo-ops' (bm/put fifo-ops thread fifo-op)]
                    (recur ctx gen' op-index' (inc outstanding) 0 fifo-ops')))))))

        (catch Throwable t
          ; We've thrown, but we still need to ensure the workers exit.
          (info "Shutting down workers after abnormal exit")
          ; We only try to cancel each worker *once*--if we try to cancel
          ; multiple times, we might interrupt a worker while it's in the
          ; finally block, cleaning up its client.
          (dorun (map (comp future-cancel :future) workers))
          ; If for some reason *that* doesn't work, we ask them all to exit via
          ; their queue.
          (loop [unfinished workers]
            (when (seq unfinished)
              (let [{:keys [in future] :as worker} (first unfinished)]
                (if (future-done? future)
                  (recur (next unfinished))
                  (do (.offer ^java.util.Queue in {:type :exit})
                      (recur unfinished))))))
          (throw t))))))

(defn run-case!
  "Takes a test with a store handle. Spawns nemesis and clients, runs the
  generator, and returns test with no :generator and a completed :history."
  [test]
  (jepsen/with-client+nemesis-setup-teardown [test test]
    (interpret! test)))

(defn notify-test-checked!
  "Complets the final check fifo op once the test is complete. Returns test
  unchanged."
  [test antithesis-phase]
  (let [fifo-op @antithesis-phase]
    (when (instance? IFifoOp fifo-op)
      (complete-fifo-op! fifo-op 0 "checked")))
  test)

(defn run!
  "Runs a test, taking direction from fifo-watcher. See jepsen.core/run!"
  [test]
  (with-thread-name "jepsen test runner"
    (let [antithesis-phase (atom :main)
          test (-> test
                   jepsen/prepare-test
                   (assoc :antithesis-phase antithesis-phase
                          :fifo-queue       (fifo-queue))
                   (update :nonserializable-keys conj
                           :antithesis-phase
                           :fifo-queue))]
      (jepsen/with-logging test
        (store/with-handle [test test]
          ; Initial save
          (let [test (store/save-0! test)]
            (util/with-relative-time
              (with-fifo-watcher! test
                (let [test (-> (run-case! test)
                               ; Remove state
                               (dissoc :barrier
                                       :sessions
                                       :antithesis-phase))
                      _ (info "Run complete, writing")]
                  (-> test
                      store/save-1!
                      jepsen/analyze!
                      jepsen/log-results
                      (notify-test-checked! antithesis-phase)))))))))))
