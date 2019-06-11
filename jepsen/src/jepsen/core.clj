(ns jepsen.core
  "Entry point for all Jepsen tests. Coordinates the setup of servers, running
  tests, creating and resolving failures, and interpreting results.

  Jepsen tests a system by running a set of singlethreaded *processes*, each
  representing a single client in the system, and a special *nemesis* process,
  which induces failures across the cluster. Processes choose operations to
  perform based on a *generator*. Each process uses a *client* to apply the
  operation to the distributed system, and records the invocation and
  completion of that operation in the *history* for the test. When the test is
  complete, a *checker* analyzes the history to see if it made sense.

  Jepsen automates the setup and teardown of the environment and distributed
  system by using an *OS* and *client* respectively. See `run!` for details."
  (:refer-clojure :exclude [run!])
  (:use     clojure.tools.logging)
  (:require [clojure.stacktrace :as trace]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [dom-top.core :as dt]
            [knossos.op :as op]
            [knossos.history :as history]
            [jepsen.util :as util :refer [with-thread-name
                                          fcatch
                                          real-pmap
                                          relative-time-nanos]]
            [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.control :as control]
            [jepsen.generator :as generator]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.nemesis :as nemesis]
            [jepsen.store :as store]
            [tea-time.core :as tt]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent CyclicBarrier
                                 CountDownLatch
                                 TimeUnit)))

(defn synchronize
  "A synchronization primitive for tests. When invoked, blocks until all nodes
  have arrived at the same point.

  This is often used in IO-heavy DB setup code to ensure all nodes have
  completed some phase of execution before moving on to the next. However, if
  an exception is thrown by one of those threads, the call to `synchronize`
  will deadlock! To avoid this, we include a default timeout of 60 seconds,
  which can be overridden by passing an alternate timeout in seconds."
  ([test]
   (synchronize test 60))
  ([test timeout-s]
   (or (= ::no-barrier (:barrier test))
       (.await ^CyclicBarrier (:barrier test) timeout-s TimeUnit/SECONDS))))

(defn conj-op!
  "Add an operation to a tests's history, and returns the operation."
  [test op]
  (swap! (:history test) conj op)
  op)

(defn primary
  "Given a test, returns the primary node."
  [test]
  (first (:nodes test)))

(defmacro with-resources
  "Takes a four-part binding vector: a symbol to bind resources to, a function
  to start a resource, a function to stop a resource, and a sequence of
  resources. Then takes a body. Starts resources in parallel, evaluates body,
  and ensures all resources are correctly closed in the event of an error."
  [[sym start stop resources] & body]
  ; Start resources in parallel
  `(let [~sym (doall (real-pmap (fcatch ~start) ~resources))]
     (when-let [ex# (some #(when (instance? Exception %) %) ~sym)]
       ; One of the resources threw instead of succeeding; shut down all which
       ; started OK and throw.
       (->> ~sym
            (remove (partial instance? Exception))
            (real-pmap (fcatch ~stop))
            dorun)
       (throw ex#))

     ; Run body
     (try ~@body
       (finally
         ; Clean up resources
         (dorun (real-pmap (fcatch ~stop) ~sym))))))

(defmacro with-os
  "Wraps body in OS setup and teardown."
  [test & body]
  `(try
     (control/on-nodes ~test (partial os/setup! (:os ~test)))
     ~@body
     (finally
       (control/on-nodes ~test (partial os/teardown! (:os ~test))))))

(defn snarf-logs!
  "Downloads logs for a test. Updates symlinks."
  [test]
  ; Download logs
  (locking snarf-logs!
    (when (satisfies? db/LogFiles (:db test))
      (info "Snarfing log files")
      (control/on-nodes test
        (fn [test node]
          (let [full-paths (db/log-files (:db test) test node)
                ; A map of full paths to short paths
                paths      (->> full-paths
                                (map #(str/split % #"/"))
                                util/drop-common-proper-prefix
                                (map (partial str/join "/"))
                                (zipmap full-paths))]
            (doseq [[remote local] paths]
              (info "downloading" remote "to" local)
              (try
                (control/download
                  remote
                  (.getCanonicalPath
                    (store/path! test (name node)
                                 ; strip leading /
                                 (str/replace local #"^/" ""))))
                (catch java.io.IOException e
                  (if (= "Pipe closed" (.getMessage e))
                    (info remote "pipe closed")
                    (throw e)))
                (catch java.lang.IllegalArgumentException e
                  ; This is a jsch bug where the file is just being
                  ; created
                  (info remote "doesn't exist"))))))))
    (store/update-symlinks! test)))

(defn maybe-snarf-logs!
  "Snarfs logs, swallows and logs all throwables. Why? Because we do this when
  we encounter an error and abort, and we don't want an error here to supercede
  the root cause that made us abort."
  [test]
  (try (snarf-logs! test)
       (catch clojure.lang.ExceptionInfo e
         (warn e (str "Error snarfing logs and updating symlinks\n")
               (with-out-str (pprint (ex-data e)))))
       (catch Throwable t
         (warn t "Error snarfing logs and updating symlinks"))))

(defmacro with-log-snarfing
  "Evaluates body and ensures logs are snarfed afterwards. Will also download
  logs in the event of JVM shutdown, so you can ctrl-c a test and get something
  useful."
  [test & body]
  `(let [^Thread hook# (Thread.
                         (bound-fn []
                           (with-thread-name "Jepsen shutdown hook"
                             (info "Downloading DB logs before JVM shutdown...")
                             (snarf-logs! ~test)
                             (store/update-symlinks! ~test))))]
     (.. (Runtime/getRuntime) (addShutdownHook hook#))
     (try
       (let [res# (do ~@body)]
         (snarf-logs! ~test)
         res#)
       (finally
         (maybe-snarf-logs! ~test)
         (.. (Runtime/getRuntime) (removeShutdownHook hook#))))))

(defmacro with-db
  "Wraps body in DB setup and teardown."
  [test & body]
  `(try
     (with-log-snarfing ~test
       (db/cycle! ~test)
       ~@body)
     (finally
       (control/on-nodes ~test (partial db/teardown! (:db ~test))))))

(defprotocol Worker
  "Polymorphic lifecycle for worker threads; synchronized setup, run, and
  teardown phases, each with error recovery. Workers are singlethreaded and may
  be stateful. Return value are ignored."
  (worker-name      [worker])
  (abort-worker!    [worker]) ; Lets a worker know it should abort
  (setup-worker!    [worker])
  (run-worker!      [worker])
  (teardown-worker! [worker]))

(defn run-workers!
  "Runs a set of workers through setup, running, and teardown."
  [workers]
  (try
    ; Set up
    (real-pmap (fn setup [w]
                 (let [name (worker-name w)]
                   (with-thread-name (str "jepsen " name)
                     (info "Setting up" name)
                     (setup-worker! w))))
               workers)
    ; Run
    (real-pmap (fn run [w]
                 (let [name (worker-name w)]
                   (with-thread-name (str "jepsen " name)
                     (info "Running" name)
                     (run-worker! w))))
               workers)

    (finally
      ; Teardown
      (real-pmap (fn teardown [w]
                   (let [name (worker-name w)]
                     (with-thread-name (str "jepsen " name)
                       (info "Tearing down" name)
                       (teardown-worker! w))))
                 workers))))

(defn invoke-op!
  "Applies an operation to a client, catching client exceptions and converting
  them to infos. Returns a completion op, throwing if the completion is
  invalid."
  [op test client abort?]
  (let [completion (try (-> (client/invoke! client test op)
                            (assoc :time (relative-time-nanos)))
                        (catch Throwable e
                          (when @abort? (throw e))

                          ; Yes, we want Throwable here: assertion errors
                          ; are not Exceptions. D-:
                          (warn e "Process" (:process op) "crashed")

                          ; Construct info from exception
                          (assoc op
                                 :type :info
                                 :time (relative-time-nanos)
                                 :error (str "indeterminate: "
                                             (if (.getCause e)
                                               (.. e getCause getMessage)
                                               (.getMessage e))))))]
    ; Validate completion
    (let [t (:type completion)]
      (assert (or (= t :ok)
                  (= t :fail)
                  (= t :info))
              (str "Expected client/invoke! to return a map with :type :ok, :fail, or :info, but received "
                   (pr-str completion) " instead")))
    (assert (= (:process op) (:process completion)))
    (assert (= (:f op)       (:f completion)))

    ; Looks good!
    completion))

(defn nemesis-invoke-op!
  "Applies an operation to a nemesis, catching exceptions and converting
  them to infos. Returns a completion op, throwing if the completion is
  invalid."
  [op test client abort?]
  (let [completion (try (-> (nemesis/invoke-compat! client test op)
                            (assoc :time (relative-time-nanos)))
                        (catch Throwable e
                          (when @abort? (throw e))

                          ; Yes, we want Throwable here: assertion errors
                          ; are not Exceptions. D-:
                          (warn e "Process" (:process op) "crashed")

                          ; Construct info from exception
                          (assoc op
                                 :type :info
                                 :time (relative-time-nanos)
                                 :error (str "indeterminate: "
                                             (if (.getCause e)
                                               (.. e getCause getMessage)
                                               (.getMessage e))))))]
    ; Validate completion
    (assert (= (:type completion) :info)
            (str "Expected nemesis/invoke! to return a map with :type :ok, :fail, or :info, but received "
                 (pr-str completion) " instead"))
    (assert (= (:process op) (:process completion)))
    (assert (= (:f op)       (:f completion)))

    ; Looks good!
    completion))

(defn nemesis-apply-op!
  "Logs, journals, and invokes an operation, logging and journaling its
  completion, and returning the completed operation."
  [op test nemesis abort?]
  (let [histories (:active-histories test)]
    (util/log-op op)
    (doseq [history @histories]
      (swap! history conj op))
    (let [completion (nemesis-invoke-op! op test nemesis abort?)]
      (doseq [history @histories]
        (swap! history conj completion))
      (util/log-op completion)
      completion)))

(deftype ClientWorker
  [test
   node
   worker-number
   ^:unsynchronized-mutable process
   ^:unsynchronized-mutable client
   abort?]

  Worker
  (worker-name [this]
    (str "worker " worker-number))

  (abort-worker! [this]
    (reset! abort? true))

  (setup-worker! [this]
    ; Create an initial client and perform setup
    (set! client (client/open-compat! (:client test) test node)))

  (run-worker! [this]
    (let [gen (:generator test)]
      (loop []
        (when @abort?
          (throw+ {:type :worker-abort}))

        (when-let [op (generator/op-and-validate gen test process)]
          (let [op (assoc op
                          :process process
                          :time    (relative-time-nanos))]
            ; We log here so users know what's going on, but wait to journal
            ; the op to the history until the last possible moment.
            (util/log-op op)

            ; Ensure a client exists
            (when-not client
              (try
                ; Open a new client
                (set! (.client this) (client/open! (:client test) test node))
                (catch Exception e
                  (warn e "Error opening client")
                  (let [fail (assoc op
                                    :type  :fail
                                    :error [:no-client
                                            (.getMessage e)]
                                    :time  (relative-time-nanos))]
                    (conj-op! test op)
                    (conj-op! test fail)
                    (util/log-op fail)
                    (set! (.client this) nil)))))

            ; If we have a client, we can go on to process the op.
            (when client
              ; Note that client creation can't have affected the state, so we
              ; defer journaling the operation until the last possible moment.
              (conj-op! test op)
              (let [completion (invoke-op! op test client abort?)]
                (conj-op! test completion)
                (util/log-op completion)
                (when (op/info? completion)
                  ; At this point all bets are off. If the client or network or
                  ; DB crashed before doing anything; this operation won't be a
                  ; part of the history. On the other hand, the DB may have
                  ; applied this operation and we *don't know* about it; e.g.
                  ; because of timeout.
                  ;
                  ; This process is effectively hung; it can not initiate a new
                  ; operation without violating the single-threaded process
                  ; constraint. We cycle to a new process identifier, and leave
                  ; the invocation uncompleted in the history.
                  (set! process (+ process (:concurrency test)))

                  (when (client/closable? client)
                    ; We can close this client and open a new one to replace
                    ; it.
                    (client/close! client test)
                    (set! client nil))))))

          ; On to the next op
          (recur)))))

  (teardown-worker! [this]
    (when client
      (client/close-compat! client test))))

(defn client-worker
  "A worker for executing operations on clients. Takes a test, an initial
  process id, and a node to bind clients to."
  [test process-id node]
  (ClientWorker. test node process-id process-id nil (atom false)))

(deftype NemesisWorker [test ^:unsynchronized-mutable nemesis abort?]
  Worker
  (worker-name [this] "nemesis")

  (abort-worker! [this]
    (reset! abort? true))

  (setup-worker! [this]
    (set! nemesis (nemesis/setup-compat! (:nemesis test) test nil)))

  (run-worker! [this]
    (let [gen (:generator test)]
      (loop []
        (when @abort?
          (throw+ {:type :worker-abort}))

        (when-let [op (generator/op-and-validate gen test :nemesis)]
          (let [completion (-> op
                               (assoc :process :nemesis
                                      :time    (relative-time-nanos))
                               (nemesis-apply-op! test nemesis abort?))]
            ; We don't do anything to recover nemeses on crash
            (recur))))))

  (teardown-worker! [this]
    (when nemesis
      (nemesis/teardown-compat! nemesis test))))

(defn nemesis-worker
  "A worker for introducing failures. Takes a test."
  [test]
  (NemesisWorker. test nil (atom false)))

(defn run-case!
  "Spawns nemesis and clients, runs a single test case, and
  returns that case's history."
  [test]
  (let [history (atom [])
        test    (assoc test :history history)]

    ; Register history with test's active set.
    (swap! (:active-histories test) conj history)

    (let [client-nodes (if (empty? (:nodes test))
                         ; If you gave us an empty node set, we'll
                         ; still give you :concurrency client, but
                         ; with nil nodes.
                         (repeat (:concurrency test) nil)
                         (->> test
                              :nodes
                              cycle
                              (take (:concurrency test))))
          clients (mapv (partial client-worker test)
                        (iterate inc 0) ; Process IDs
                        client-nodes)
          nemesis (nemesis-worker test)]
      ; Go!
      (run-workers! (cons nemesis clients)))

    ; Unregister our history
    (swap! (:active-histories test) disj history)

    @history))

(defn analyze!
  "After running the test and obtaining a history, we perform some
  post-processing on the history, run the checker, and write the test to disk
  again."
  [test]
  (info "Analyzing...")
  (let [; Give each op in the history a monotonically increasing index
        test (assoc test :history (history/index (:history test)))
        _ (when (:model test)
            (warn "DEPRECATED: Checker model is assigned to test, which is no longer supported. If the checker still needs a model, see `jepsen.checker` documentation for details."))
        ; Run checkers
        test (assoc test :results (checker/check-safe
                                   (:checker test)
                                   test
                                   (:history test)))]
    (info "Analysis complete")
    (when (:name test) (store/save-2! test))
    test))

(defn log-results
  "Logs info about the results of a test to stdout, and returns test."
  [test]
  (info (str
          (with-out-str
            (pprint (:results test)))
          (when (:error (:results test))
            (str "\n\n" (:error (:results test))))
          "\n\n"
          (case (:valid? (:results test))
            false     "Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻"
            :unknown  "Errors occurred during analysis, but no anomalies found. ಠ~ಠ"
            true      "Everything looks good! ヽ(‘ー`)ノ")))
  test)

(defn run!
  "Runs a test. Tests are maps containing

  :nodes      A sequence of string node names involved in the test
  :concurrency  (optional) How many processes to run concurrently
  :ssh        SSH credential information: a map containing...
    :username           The username to connect with   (root)
    :password           The password to use
    :port               SSH listening port (22)
    :private-key-path   A path to an SSH identity file (~/.ssh/id_rsa)
    :strict-host-key-checking  Whether or not to verify host keys
  :logging    Logging options; see jepsen.store/start-logging!
  :os         The operating system; given by the OS protocol
  :db         The database to configure: given by the DB protocol
  :client     A client for the database
  :nemesis    A client for failures
  :generator  A generator of operations to apply to the DB
  :checker    Verifies that the history is valid
  :log-files  A list of paths to logfiles/dirs which should be captured at
              the end of the test.
  :nonserializable-keys   A collection of top-level keys in the test which
                          shouldn't be serialized to disk.

  Tests proceed like so:

  1. Setup the operating system

  2. Try to teardown, then setup the database
    - If the DB supports the Primary protocol, also perform the Primary setup
      on the first node.

  3. Create the nemesis

  4. Fork the client into one client for each node

  5. Fork a thread for each client, each of which requests operations from
     the generator until the generator returns nil
    - Each operation is appended to the operation history
    - The client executes the operation and returns a vector of history elements
      - which are appended to the operation history

  6. Capture log files

  7. Teardown the database

  8. Teardown the operating system

  9. When the generator is finished, invoke the checker with the history
    - This generates the final report"
  [test]
  (tt/with-threadpool
    (try
      (with-thread-name "jepsen test runner"
        (let [test (assoc test
                          ; Initialization time
                          :start-time (util/local-time)

                          ; Number of concurrent workers
                          :concurrency (or (:concurrency test)
                                           (count (:nodes test)))

                          ; Synchronization point for nodes
                          :barrier (let [c (count (:nodes test))]
                                     (if (pos? c)
                                       (CyclicBarrier. (count (:nodes test)))
                                       ::no-barrier))

                          ; Currently running histories
                          :active-histories (atom #{}))
              _    (store/start-logging! test)
              _    (info "Running test:\n" (with-out-str (pprint test)))
              test (control/with-ssh (:ssh test)
                     (with-resources [sessions
                                      (bound-fn* control/session)
                                      control/disconnect
                                      (:nodes test)]
                       ; Index sessions by node name and add to test
                       (let [test (->> sessions
                                       (map vector (:nodes test))
                                       (into {})
                                       (assoc test :sessions))]
                         ; Setup
                         (with-os test
                           (with-db test
                             (generator/with-threads
                               (cons :nemesis (range (:concurrency test)))
                               (util/with-relative-time
                                 ; Run a single case
                                 (let [test (assoc test :history
                                                   (run-case! test))
                                       ; Remove state
                                       test (dissoc test
                                                    :barrier
                                                    :active-histories
                                                    :sessions)]
                                   (info "Run complete, writing")
                                   (when (:name test) (store/save-1! test))
                                   (analyze! test)))))))))]
          (log-results test)))
      (catch Throwable t
        (warn t "Test crashed!")
        (throw t))
    (finally
      (store/stop-logging!)))))
