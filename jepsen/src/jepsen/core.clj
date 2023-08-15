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
  (:require [clojure.java.shell :refer [sh]]
            [clojure.stacktrace :as trace]
            [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [clojure.datafy :refer [datafy]]
            [dom-top.core :as dt :refer [assert+]]
            [fipp.edn :refer [pprint]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [control :as control]
                    [db :as db]
                    [generator :as generator]
                    [nemesis :as nemesis]
                    [store :as store]
                    [os :as os]
                    [util :as util :refer [with-thread-name
                                          fcatch
                                          real-pmap
                                          relative-time-nanos]]]
            [jepsen.store.format :as store.format]
            [jepsen.control.util :as cu]
            [jepsen.generator [interpreter :as gen.interpreter]]
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
          (doseq [[remote local] (db/log-files-map (:db test) test node)]
            (when (cu/exists? remote)
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
  `(let [^Runnable hook-fn#
         (bound-fn []
           (with-thread-name "Jepsen shutdown hook"
             (info "Downloading DB logs before JVM shutdown...")
             (snarf-logs! ~test)
             (store/update-symlinks! ~test)))

         ^Thread hook# (Thread. hook-fn#)]
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
       (when-not (:leave-db-running? ~test)
         (control/on-nodes ~test (partial db/teardown! (:db ~test)))))))

(defmacro with-client+nemesis-setup-teardown
  "Takes a binding vector of a test symbol and a test map. Sets up clients and
  nemesis, and rebinds (:nemesis test) to the set-up nemesis. Evaluates body.
  Afterwards, ensures clients and nemesis are torn down."
  [[test-sym test] & body]
  `(let [client#  (:client ~test)
         nemesis# (nemesis/validate (:nemesis ~test))]
    ; Setup
    (let [nf# (future (nemesis/setup! nemesis# ~test))
               clients# (real-pmap (fn [node#]
                                     (with-thread-name
                                       (str "jepsen node " node#)
                                       (let [c# (client/open! client# ~test node#)]
                                         (client/setup! c# ~test)
                                         c#)))
                                   (:nodes ~test))
               nemesis# @nf#
               ~test-sym (assoc ~test :nemesis nemesis#)]
      (try
        (dorun clients#)
        ~@body
        (finally
          ; Teardown (and close clients)
          (let [nf# (future (nemesis/teardown! nemesis# ~test))]
            (dorun (real-pmap (fn [[c# node#]]
                                (with-thread-name
                                  (str "jepsen node " node#))
                                (try (client/teardown! c# ~test)
                                     (finally
                                       (client/close! c# ~test))))
                              (map vector clients# (:nodes ~test))))
            @nf#))))))

(defn run-case!
  "Takes a test with a store handle. Spawns nemesis and clients and runs the
  generator. Returns test with no :generator and a completed :history."
  [test]
  (with-client+nemesis-setup-teardown [test test]
    (gen.interpreter/run! test)))

(defn analyze!
  "After running the test and obtaining a history, we perform some
  post-processing on the history, run the checker, and write the test to disk
  again. Takes a test map. Returns a new test with results."
  [test]
  (info "Analyzing...")
  (let [test (assoc test :results (checker/check-safe
                                    (:checker test)
                                    test
                                    (:history test)))]
    (info "Analysis complete")
    (if (:name test)
      (store/save-2! test)
      test)))

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

(defn log-test-start!
  "Logs some basic information at the start of a test: the Git version of the
  working directory, the lein arguments to re-run the test, etc."
  [test]
  (let [git-head (sh "git" "rev-parse" "HEAD")]
    (when (zero? (:exit git-head))
      (let [head      (str/trim-newline (:out git-head))
            clean? (-> (sh "git" "status" "--porcelain=v1")
                       :out
                       str/blank?)]
        (info (str "Test version " head
                   (when-not clean? " (plus uncommitted changes)"))))))
  (when-let [argv (:argv test)]
    (info (str "Command line:\n"
          (->> (:argv test)
               (map control/escape)
               (list* "lein" "run")
               (str/join " ")))))
  (info (str "Running test:\n"
             (util/test->str test))))

(defmacro with-sessions
  "Takes a [test' test] binding form and a body. Starts with test-expr as
  the test, and sets up the jepsen.control state required to run this test--the
  remote, SSH options, etc. Opens SSH sessions to each node. Saves those
  sessions in the :sessions map of the test, binds that to the `test'` symbol in
  the binding expression, and evaluates body."
  [[test' test] & body]
  `(let [test# ~test]
     (control/with-remote (:remote test#)
       (control/with-ssh (:ssh test#)
         (with-resources [sessions#
                          (bound-fn* control/session)
                          control/disconnect
                          (:nodes test#)]
           ; Index sessions by node name and add to test
           (let [~test' (->> sessions#
                             (map vector (:nodes test#))
                             (into {})
                             (assoc test# :sessions))]
                 ; And evaluate body.
                 ~@body))))))

(defmacro with-logging
  "Sets up logging for this test run, logs the start of the test, evaluates
  body, and stops logging at the end. Also logs test crashes, so they appear in
  the log files for this test run."
  [test & body]
  `(try (store/start-logging! ~test)
        (log-test-start! ~test)
        ~@body
        (catch Throwable t#
          (warn t# "Test crashed!")
          (throw t#))
        (finally
          (store/stop-logging!))))

(defn prepare-test
  "Takes a test and prepares it for running. Ensures it has a :start-time,
  :concurrency, and :barrier field. Wraps its generator in a forgettable
  reference, to prevent us from inadvertently retaining the head.

  This operation always succeeds, and is necessary for accessing a test's store
  directory, which depends on :start-time. You may call this yourself before
  calling run!, if you need access to the store directory outside the run!
  context."
  [test]
  (cond-> test
    (not (:start-time test)) (assoc :start-time (util/local-time))
    (not (:concurrency test)) (assoc :concurrency (count (:nodes test)))
    (not (:barrier test)) (assoc :barrier
                                 (let [c (count (:nodes test))]
                                   (if (pos? c)
                                     (CyclicBarrier. (count (:nodes test)))
                                     ::no-barrier)))
    true (update :generator util/forgettable)))

(defn run!
  "Runs a test. Tests are maps containing

    :nodes      A sequence of string node names involved in the test
    :concurrency  (optional) How many processes to run concurrently
    :ssh        SSH credential information: a map containing...
      :username           The username to connect with   (root)
      :password           The password to use
      :sudo-password      The password to use for sudo, if needed
      :port               SSH listening port (22)
      :private-key-path   A path to an SSH identity file (~/.ssh/id_rsa)
      :strict-host-key-checking  Whether or not to verify host keys
    :logging    Logging options; see jepsen.store/start-logging!
    :os         The operating system; given by the OS protocol
    :db         The database to configure: given by the DB protocol
    :remote     The remote to use for control actions. Try, for example,
                (jepsen.control.sshj/remote).
    :client     A client for the database
    :nemesis    A client for failures
    :generator  A generator of operations to apply to the DB
    :checker    Verifies that the history is valid
    :log-files  A list of paths to logfiles/dirs which should be captured at
                the end of the test.
    :nonserializable-keys   A collection of top-level keys in the test which
                            shouldn't be serialized to disk.
    :leave-db-running? Whether to leave the DB running at the end of the test.

  Jepsen automatically adds some additional keys during the run

    :start-time     When the test began
    :history        The operations the clients and nemesis performed
    :results        The results from the checker, once the test is completed

  In addition, tests have some fields added by Jepsen which are present during
  their execution, but not persisted.

    :barrier        A CyclicBarrier, mainly used for synchronizing DB setup
    :store          State used for reading and writing data to and from disk
    :sessions       Connected sessions used by jepsen.control to talk to nodes

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
  (with-thread-name "jepsen test runner"
    (let [test (prepare-test test)]
      (with-logging test
        (store/with-handle [test test]
          (let [test (if (:name test)
                       (store/save-0! test)
                       test)
                test (with-sessions [test test]
                       ; Launch OS, DBs, evaluate test
                       (let [test (with-os test
                                    (with-db test
                                      (util/with-relative-time
                                        ; Run a single case
                                        (let [test (-> (run-case! test)
                                                       ; Remove state
                                                       (dissoc :barrier
                                                               :sessions))
                                              _ (info "Run complete, writing")
                                              test (if (:name test)
                                                     (store/save-1! test)
                                                     test)]
                                          test))))]
                         (analyze! test)))]
            (log-results test)))))))
