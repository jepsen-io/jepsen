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
            [knossos.core :as knossos]
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
            [jepsen.store :as store])
  (:import (java.util.concurrent CyclicBarrier)))

(defn synchronize
  "A synchronization primitive for tests. When invoked, blocks until all
  nodes have arrived at the same point."
  [test]
  (or (= ::no-barrier (:barrier test))
      (.await ^CyclicBarrier (:barrier test))))

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

(defn setup-primary!
  "Given a test, sets up the database primary, if the DB supports it."
  [test]
  (when (satisfies? db/Primary (:db test))
    (let [p (primary test)]
      (control/with-session p (get-in test [:sessions p])
        (db/setup-primary! (:db test) test p)))))

(defn snarf-logs!
  "Downloads logs for a test."
  [test]
  ; Download logs
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
                        (info remote "doesn't exist")))))))))

(defmacro with-db
  "Wraps body in DB setup and teardown."
  [test & body]
  `(try
     (control/on-nodes ~test (partial db/cycle! (:db ~test)))
     (setup-primary! ~test)

     ~@body
     (catch Throwable t#
       ; Emergency log dump!
       (snarf-logs! ~test)
       (store/update-symlinks! ~test)
       (throw t#))
     (finally
       (control/on-nodes ~test (partial db/teardown! (:db ~test))))))

(defn worker
  "Spawns a future to execute a particular process in the history."
  [test process client]
  (let [gen (:generator test)]
    (future
      (with-thread-name (str "jepsen worker " process)
        (info "Worker" process "starting")
        (loop [process process]
          ; Obtain an operation to execute
          (when-let [op (generator/op gen test process)]
            (assert (map? op) (str "Expected an operation map from " gen
                                   ", but got " (pr-str op) " instead."))
            (let [op (assoc op
                            :process process
                            :time    (relative-time-nanos))]
              ; Log invocation
              (util/log-op op)
              (conj-op! test op)

              (recur
                (try
                  ; Evaluate operation
                  (let [completion (-> (client/invoke! client test op)
                                       (assoc :time (relative-time-nanos)))]
                    (util/log-op completion)

                    ; Sanity checks
                    (let [t (:type completion)]
                      (assert (or (= t :ok)
                                  (= t :fail)
                                  (= t :info))
                              (str "Expected client/invoke! to return a map with :type :ok, :fail, or :info, but received " (pr-str completion) " instead")))
                    (assert (= (:process op) (:process completion)))
                    (assert (= (:f op)       (:f completion)))

                    ; Log completion
                    (conj-op! test completion)

                    (if (or (knossos/ok? completion) (knossos/fail? completion))
                      ; The process is now free to attempt another execution.
                      process
                      ; Process hung; move on
                      (+ process (:concurrency test))))

                  (catch Throwable t
                    ; At this point all bets are off. If the client or network
                    ; or DB crashed before doing anything; this operation won't
                    ; be a part of the history. On the other hand, the DB may
                    ; have applied this operation and we *don't know* about it;
                    ; e.g.  because of timeout.
                    ;
                    ; This process is effectively hung; it can not initiate a
                    ; new operation without violating the single-threaded
                    ; process constraint. We cycle to a new process identifier,
                    ; and leave the invocation uncompleted in the history.
                    (conj-op! test (assoc op
                                          :type :info
                                          :time  (relative-time-nanos)
                                          :error (str "indeterminate: "
                                                      (if (.getCause t)
                                                        (.. t getCause
                                                            getMessage)
                                                        (.getMessage t)))))
                    (warn t "Process" process "indeterminate")
                    (+ process (:concurrency test))))))))
        (info "Worker" process "done")))))

(defn nemesis-worker
  "Starts the nemesis thread, which draws failures from the generator and
  evaluates them. Returns a future."
  [test nemesis]
  (let [gen       (:generator        test)
        histories (:active-histories test)]
    (future
      (with-thread-name "jepsen nemesis"
        (info "Nemesis starting")
        (loop []
          (when-let [op (generator/op gen test :nemesis)]
            (assert (map? op) (str "Expected an operation map for nemesis from "
                                   gen
                                   ", but got " (pr-str op) " instead."))
            (let [op (assoc op
                            :process :nemesis
                            :time    (relative-time-nanos))]
              ; Log invocation in all histories of all currently running cases
              (doseq [history @histories]
                (swap! history conj op))

              (try
                (util/log-op op)
                (let [completion (-> (client/invoke! nemesis test op)
                                     (assoc :time (relative-time-nanos)))]
                  (util/log-op completion)

                  ; Nemesis is not allowed to affect the model
                  (assert (= (:type op)    :info))
                  (assert (= (:f op)       (:f completion)))
                  (assert (= (:process op) (:process completion)))

                  ; Log completion in all histories of all currently running
                  ; cases
                  (doseq [history @histories]
                    (swap! history conj completion)))

                (catch Throwable t
                  (doseq [history @histories]
                    (swap! history conj (assoc op
                                               :time  (relative-time-nanos)
                                               :value (str "crashed: " t))))
                  (warn t "Nemesis crashed evaluating" op)))

              (recur))))
        (info "nemesis done")))))

(defmacro with-nemesis
  "Sets up nemesis, starts nemesis worker thread, evaluates body, waits for
  nemesis completion, and tears down nemesis."
  [test & body]
  ; Initialize nemesis
  `(let [nemesis# (client/setup! (:nemesis ~test) ~test nil)]
     (try
       ; Launch nemesis thread
       (let [worker# (nemesis-worker ~test nemesis#)
             result# ~@body]
         ; Wait for nemesis worker to complete
         (info "Waiting for nemesis to complete")
         (deref worker#)
         (info "nemesis done.")
         result#)
       (finally
         (info "Tearing down nemesis")
         (client/teardown! nemesis# ~test)
         (info "Nemesis torn down")))))

(defn run-case!
  "Spawns nemesis and clients, runs a single test case, snarf the logs, and
  returns that case's history."
  [test]
  (let [history (atom [])
        test    (assoc test :history history)]

    ; Register history with test's active set.
    (swap! (:active-histories test) conj history)

    ; Launch clients
    (with-resources [clients
                     #(client/setup! (:client test) test %) ; Specialize to node
                     #(client/teardown! % test)
                     (if (empty? (:nodes test))
                       ; If you've specified an empty node set, we'll still
                       ; give you `concurrency` clients, with nil.
                       (repeat (:concurrency test) nil)
                       (->> test
                            :nodes
                            cycle
                            (take (:concurrency test))))]
      ; Launch nemesis
      (with-nemesis test
        ; Begin workload
        (let [workers (mapv (partial worker test)
                            (iterate inc 0) ; PIDs
                            clients)]       ; Clients

          ; Wait for workers to complete
          (dorun (map deref workers)))))

    ; Download logs
    (snarf-logs! test)

    ; Unregister our history
    (swap! (:active-histories test) disj history)

    @history))

(defn log-results
  "Logs info about the results of a test to stdout, and returns test."
  [test]
  (info (str
          (with-out-str
            (pprint (:results test)))
          (when (:error (:results test))
            (str "\n\n" (:error (:results test))))
          "\n\n"
          (if (:valid? (:results test))
            "Everything looks good! ヽ(‘ー`)ノ"
            "Analysis invalid! (ﾉಥ益ಥ）ﾉ ┻━┻")))
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
  :os         The operating system; given by the OS protocol
  :db         The database to configure: given by the DB protocol
  :client     A client for the database
  :nemesis    A client for failures
  :generator  A generator of operations to apply to the DB
  :model      The model used to verify the history is correct
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

  9. When the generator is finished, invoke the checker with the model and
     the history
    - This generates the final report"
  [test]
  (info "Running test:\n" (with-out-str (pprint test)))
  (try
    (log-results
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
                                 (let [test (assoc test :history (run-case! test))
                                       ; Remove state
                                       test (dissoc test
                                                    :barrier
                                                    :active-histories
                                                    :sessions)]
                                   (info "Run complete, writing")
                                   (when (:name test) (store/save-1! test))
                                   test))))))))
              _ (info "Analyzing")
              test (assoc test :results (checker/check-safe
                                          (:checker test)
                                          test
                                          (:model test)
                                          (:history test)))]

          (info "Analysis complete")
          (when (:name test) (store/save-2! test)))))
    (finally
      (store/stop-logging!))))
