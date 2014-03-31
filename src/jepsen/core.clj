(ns jepsen.core
  "Entry point for all Jepsen tests. Coordinates the setup of servers, running
  tests, creating and resolving failures, and interpreting results."
  (:use     clojure.tools.logging)
  (:require [clojure.stacktrace :as trace]
            [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.control :as control]
            [jepsen.generator :as generator]
            [jepsen.checker :as checker]
            [jepsen.client :as client])
  (:import (java.util.concurrent CyclicBarrier)))

(defn synchronize
  "A synchronization primitive for tests. When invoked, blocks until all
  nodes have arrived at the same point."
  [test]
  (.await ^CyclicBarrier (:barrier test)))

(defn primary
  "Given a test, returns the primary node."
  [test]
  (first (:nodes test)))

(defn fcatch
  "Takes a function and returns a version of it which returns, rather than
  throws, exceptions."
  [f]
  (fn wrapper [& args]
    (try (apply f args)
         (catch Exception e e))))

(defmacro with-resources
  "Takes a four-part binding vector: a symbol to bind resources to, a function
  to start a resource, a function to stop a resource, and a sequence of
  resources. Then takes a body. Starts resources in parallel, evaluates body,
  and ensures all resources are correctly closed in the event of an error."
  [[sym start stop resources] & body]
  ; Start resources in parallel
  `(let [~sym (doall (pmap (fcatch ~start) ~resources))]
     (when-let [ex# (some (partial instance? Exception) ~sym)]
       ; One of the resources threw instead of succeeding; shut down all which
       ; started OK and throw.
       (->> ~sym
            (remove (partial instance? Exception))
            (pmap (fcatch ~stop))
            dorun)
       (throw ex#))

     ; Run body
     (try ~@body
       (finally
         ; Clean up resources
         (dorun (pmap (fcatch ~stop) ~sym))))))

(defn on-nodes
  "Given a test, evaluates (f test node) in parallel on each node, with that
  node's SSH connection bound."
  [test f]
  (dorun (pmap (fn [[node session]]
                 (control/with-session node session
                   (f test node)))
               (:sessions test))))

(defmacro with-os
  "Wraps body in OS setup and teardown."
  [test & body]
  `(try
     (on-nodes ~test (partial os/setup! (:os ~test)))
     ~@body
     (finally
       (on-nodes ~test (partial os/teardown! (:os ~test))))))

(defn setup-primary!
  "Given a test, sets up the database primary, if the DB supports it."
  [test]
  (when (satisfies? db/Primary (:db test))
    (let [p (primary test)]
      (control/with-session p (get-in test [:sessions p])
        (db/setup-primary! (:db test) test p)))))

(defmacro with-db
  "Wraps body in DB setup and teardown."
  [test & body]
  `(try
     (on-nodes ~test (partial db/cycle! (:db ~test)))
     (setup-primary! ~test)

     ~@body
     (finally
       (on-nodes ~test (partial db/teardown! (:db ~test))))))

(defn worker
  "Spawns a future to execute a particular process in the history."
  [test process client]
  (let [gen (:generator test)
        hist (:history test)]
    (future
      (loop [process process]
        ; Obtain an operation to execute
        (when-let [op (generator/op gen test process)]
          (let [op (assoc op :process process)]
            ; Log invocation
            (swap! hist conj op)
            (recur
              (try
                ; Evaluate operation
                (let [completion (client/invoke! client test op)]
                  (info completion)

                  ; Sanity check
                  (assert (= (:process op) (:process completion)))
                  (assert (= (:f op)       (:f completion)))

                  ; Log completion
                  (swap! hist conj completion)

                  ; The process is now free to attempt another execution.
                  process)

                (catch Throwable t
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
                  (swap! hist conj (assoc op
                                          :type :info
                                          :value (str "indeterminate: "
                                                      (if (.getCause t)
                                                        (.. t getCause
                                                            getMessage)
                                                        (.getMessage t)))))
                  (warn t "Process" process "indeterminate")
                  (+ process (count (:nodes test))))))))))))

(defmacro with-nemesis
  "Sets up nemesis and binds to sym, evaluates body, and tears down nemesis."
  [[sym test] & body]
  ; Initialize nemesis
  `(let [~sym (client/setup! (:nemesis ~test) ~test nil)]
     (try
       ; Launch nemesis thread
       (let [worker# (worker ~test :nemesis ~sym)
             result# ~@body]
         ; Wait for nemesis worker to complete
         (deref worker#)
         result#)
       (finally
         (client/teardown! ~sym ~test)))))

(defn run-case!
  "Spawns clients, runs a single test case, and returns that case's history."
  [test]
  (let [test (assoc test :history (atom []))]
    ; Initialize clients
    (with-resources [clients
                     #(client/setup! (:client test) test %) ; Specialize to node
                     #(client/teardown! % test)
                     (:nodes test)]

      ; Begin workload
      (let [workers (mapv (partial worker test)
                          (iterate inc 0) ; PIDs
                          clients)]       ; Clients

        ; Wait for workers to complete
        (dorun (map deref workers))

        ; Return history from this case's evaluation
        (deref (:history test))))))

(defn run!
  "Runs a test. Tests are maps containing

  :nodes      A sequence of string node names involved in the test.
  :ssh        SSH credential information: a map containing...
    :username           The username to connect with   (root)
    :password           The password to use
    :private-key-path   A path to an SSH identity file (~/.ssh/id_rsa)
    :strict-host-key-checking  Whether or not to verify host keys
  :os         The operating system; given by the OS protocol
  :db         The database to configure: given by the DB protocol
  :client     A client for the database
  :nemesis    A client for failures
  :generator  A generator of operations to apply to the DB
  :model      The model used to verify the history is correct
  :checker    Verifies that the history is valid

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

  6. Teardown the database

  7. Teardown the operating system

  8. When the generator is finished, invoke the checker with the model and
     the history
    - This generates the final report"
  [test]
  ; Synchronization point for nodes
  (let [test (assoc test :barrier (CyclicBarrier. (count (:nodes test))))]

    ; Open SSH conns
    (control/with-ssh (:ssh test)
      (with-resources [sessions control/session control/disconnect
                       (:nodes test)]
        ; Index sessions by node name and add to test
        (let [test (->> sessions
                        (map vector (:nodes test))
                        (into {})
                        (assoc test :sessions))]

          ; Setup
          (with-os test
            (with-db test
              (with-nemesis [nemesis test]
                ; Run a single case
                (let [test (assoc test :history (run-case! test))]
                  (assoc test :results (checker/check-safe
                                         (:checker test)
                                         test
                                         (:model test)
                                         (:history test))))))))))))
