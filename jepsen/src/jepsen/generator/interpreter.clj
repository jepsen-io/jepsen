(ns jepsen.generator.interpreter
  "This namespace interprets operations from a pure generator, handling worker
  threads, spawning processes for interacting with clients and nemeses, and
  recording a history."
  (:refer-clojure :exclude [run!])
  (:require [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn error]]
            [jepsen [client         :as client]
                    [generator      :as gen]
                    [history        :as h]
                    [nemesis        :as nemesis]
                    [util           :as util]]
            [jepsen.generator.context :as context]
            [jepsen.store.format :as store.format]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent ArrayBlockingQueue
                                 TimeUnit)
           (io.lacuna.bifurcan Set)))


(defprotocol Worker
  "This protocol allows the interpreter to manage the lifecycle of stateful
  workers. All operations on a Worker are guaranteed to be executed by a single
  thread."
  (open [this test id]
        "Spawns a new Worker process for the given worker ID.")

  (invoke! [this test op]
           "Asks the worker to perform this operation, and returns a completed
           operation.")

  (close! [this test]
          "Closes this worker, releasing any resources it may hold."))

(deftype ClientWorker [node
                       ^:unsynchronized-mutable process
                       ^:unsynchronized-mutable client]
  Worker
  (open [this test id]
    this)

  (invoke! [this test op]
    (if (and (not= process (:process op))
             (not (client/is-reusable? client test)))
      ; New process, new client!
      (do (close! this test)
          ; Try to open new client
          (let [err (try
                      (set! (.client this)
                            (client/open! (client/validate (:client test))
                                          test node))
                      (set! (.process this) (:process op))
                     nil
                     (catch Exception e
                       (warn e "Error opening client")
                       (set! (.client this) nil)
                       (assoc op
                              :type :fail
                              :error [:no-client (.getMessage e)])))]
            ; If we failed to open, just go ahead and return that error op.
            ; Otherwise, we can try again, this time with a fresh client.
            (or err (recur test op))))
      ; Good, we have a client for this process.
      (client/invoke! client test op)))

  (close! [this test]
    (when client
      (client/close! client test)
      (set! (.client this) nil))))

(defrecord NemesisWorker []
  Worker
  (open [this test id] this)

  (invoke! [this test op]
    (nemesis/invoke! (:nemesis test) test op))

  (close! [this test]))

; This doesn't feel like the right shape exactly, but it's symmetric to Client,
; Nemesis, etc.
(defrecord ClientNemesisWorker []
  Worker
  (open [this test id]
    ;(locking *out* (prn :spawn id))
    (if (integer? id)
      (let [nodes (:nodes test)]
        (ClientWorker. (nth nodes (mod id (count nodes))) nil nil))
      (NemesisWorker.)))

  (invoke! [this test op])

  (close! [this test]))

(defn client-nemesis-worker
  "A Worker which can spawn both client and nemesis-specific workers based on
  the :client and :nemesis in a test."
  []
  (ClientNemesisWorker.))

(defn spawn-worker
  "Creates communication channels and spawns a worker thread to evaluate the
  given worker. Takes a test, a Queue which should receive completion
  operations, a Worker object, and a worker id.

  Returns a map with:

    :id       The worker ID
    :future   The future evaluating the worker code
    :in       A Queue which delivers invocations to the worker"
  [test ^ArrayBlockingQueue out worker id]
  (let [in          (ArrayBlockingQueue. 1)
        fut
        (future
          (util/with-thread-name (str "jepsen worker "
                                      (util/name+ id))
            (let [worker (open worker test id)]
              (try
                (loop []
                  (when
                    (let [op (.take in)]
                      (try
                        (case (:type op)
                          ; We're done here
                          :exit  false

                          ; Ahhh
                          :sleep (do (Thread/sleep (* 1000 (:value op)))
                                     (.put out op)
                                     true)

                          ; Log a message
                          :log   (do (info (:value op))
                                     (.put out op)
                                     true)

                          ; Ask the invoke handler
                          (do (util/log-op op)
                              (let [op' (invoke! worker test op)]
                                (.put out op')
                                (util/log-op op')
                                true)))

                        (catch Throwable e
                          ; Yes, we want to capture throwable here;
                          ; assertion errors aren't Exceptions. D-:
                          (warn e "Process" (:process op) "crashed")

                          ; Convert this to an info op.
                          (.put out
                                (assoc op
                                       :type      :info
                                       :exception (datafy e)
                                       :error     (str "indeterminate: "
                                                       (if (.getCause e)
                                                         (.. e getCause
                                                             getMessage)
                                                         (.getMessage e)))))
                          true)))
                    (recur)))
                (finally
                  ; Make sure we close our worker on exit.
                  (close! worker test))))))]
    {:id      id
     :in      in
     :future  fut}))

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
  "Takes a test with a :store :handle open. Causes the test's reference to the
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
          completions (ArrayBlockingQueue.
                        (.size ^io.lacuna.bifurcan.ISet worker-ids))
          workers     (mapv (partial spawn-worker test completions
                                     (client-nemesis-worker))
                            worker-ids)
          invocations (into {} (map (juxt :id :in) workers))
          gen         (->> (:generator test)
                           deref
                           gen/friendly-exceptions
                           gen/validate)
          ; Forget generator
          _           (util/forget! (:generator test))
          test        (dissoc test :generator)]
      (try+
        (loop [ctx            ctx
               gen            gen
               op-index       0     ; Index of the next op in the history
               outstanding    0     ; Number of in-flight ops
               ; How long to poll on the completion queue, in micros.
               poll-timeout   0]
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
                  ctx     (context/free-thread ctx time thread)
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
                            (context/with-next-process ctx thread)
                            ctx)]
              ; Log completion and move on
              (if (goes-in-history? op')
                (do (store.format/append-to-big-vector-block!
                      history-writer op')
                    (recur ctx gen (inc op-index) (dec outstanding) 0))
                (recur ctx gen op-index (dec outstanding) 0)))

            ; There's nothing to complete; let's see what the generator's up to
            (let [time        (util/relative-time-nanos)
                  ctx         (assoc ctx :time time)
                  ;_ (prn :asking-for-op)
                  ;_ (binding [*print-length* 12] (pprint gen))
                  [op gen']   (gen/op gen test ctx)]
              ;_ (prn :time time :got op)]
              (condp = op
                ; We're exhausted, but workers might still be going.
                nil (if (pos? outstanding)
                      ; Still waiting on workers
                      (recur ctx gen op-index outstanding
                             (long max-pending-interval))
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
                :pending (recur ctx gen op-index
                                outstanding (long max-pending-interval))

                ; Good, we've got an invocation.
                (if (< time (:time op))
                  ; Can't evaluate this op yet!
                  (do ;(prn :waiting (util/nanos->secs (- (:time op) time)) "s")
                      (recur ctx gen op-index outstanding
                             ; Unless something changes, we don't need to ask
                             ; the generator for another op until it's time.
                             (long (/ (- (:time op) time) 1000))))

                  ; Good, we can run this.
                  (let [thread (gen/process->thread ctx (:process op))
                        op (assoc op :index op-index)
                        ; Log the invocation
                        goes-in-history? (goes-in-history? op)
                        _ (when goes-in-history?
                            (store.format/append-to-big-vector-block!
                              history-writer op))
                        op-index' (if goes-in-history? (inc op-index) op-index)
                        ; Dispatch it to a worker
                        _ (.put ^ArrayBlockingQueue (get invocations thread) op)
                        ; Update our context to reflect
                        ctx (context/busy-thread ctx
                                                 (:time op) ; Use time instead?
                                                 thread)
                        ; Let the generator know about the invocation
                        gen' (gen/update gen' test ctx op)]
                    (recur ctx gen' op-index' (inc outstanding) 0)))))))

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
