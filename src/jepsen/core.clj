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
                  (swap! hist conj (assoc op :type  :info
                                          :value (str "indeterminate: "
                                                      (.getMessage t))))
                  (warn t "Process" process "indeterminate")
                  (+ process (count (:nodes test))))))))))))

(defn on-nodes
  "Given a map of nodes to SSH sessions and a test, evaluates (f test node) in
  parallel on each node, with that node's SSH connection bound."
  [test sessions f]
  (dorun (pmap (fn [[node session]]
                 (control/with-session node session
                   (f test node)))
               sessions)))

(defn synchronize
  "A synchronization primitive for tests. When invoked, blocks until all
  nodes have arrived at the same point."
  [test]
  (.await ^CyclicBarrier (:barrier test)))

(defn primary
  "Given a test, returns the primary node."
  [test]
  (first (:nodes test)))

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
  ; Create history
  (let [test (assoc test
                    ; The history of operations executed during the test
                    :history (atom [])
                    ; Synchronization point for nodes
                    :barrier (CyclicBarrier. (count (:nodes test))))]

    (control/with-ssh (:ssh test)
      ; Open SSH conns
      (let [sessions (->> test
                          :nodes
                          (pmap (juxt identity control/session))
                          (into {}))]

        (try
          ; Setup
          (on-nodes test sessions (partial os/setup! (:os test)))
          (on-nodes test sessions (partial db/cycle! (:db test)))

          ; Primary setup
          (when (satisfies? db/Primary (:db test))
            (let [p (primary test)]
            (control/with-session p (get sessions p)
              (db/setup-primary! (:db test) test p))))

          ; Initialize nemesis and clients
          (let [nemesis (client/setup! (:nemesis test) test nil)
                clients (mapv (partial client/setup! (:client test) test)
                              (:nodes test))

                ; Begin workload
                workers (mapv (partial worker test)
                              (cons :nemesis (iterate inc 0)) ; Process IDs
                              (cons nemesis clients))]        ; Clients

            ; Wait for workers to complete
            (dorun (map deref workers))

            ; Teardown
            (client/teardown! nemesis test nil)
            (dorun (pmap #(client/teardown! %1 test %2) clients (:nodes test)))

            (on-nodes test sessions (partial db/teardown! (:db test)))
            (on-nodes test sessions (partial os/teardown! (:os test)))

            ; Seal history and check
            (let [test    (assoc test :history (deref (:history test)))
                  results (try (checker/check (:checker test)
                                              test
                                              (:model test)
                                              (:history test))
                               (catch Throwable t
                                 {:valid? false
                                  :error (with-out-str
                                           (trace/print-cause-trace t))}))]
              (assoc test :results results)))

          (finally
            ; Make sure we close the SSH sessions
            (dorun (pmap control/disconnect (vals sessions)))))))))
