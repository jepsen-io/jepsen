(ns jepsen.core
  "Entry point for all Jepsen tests. Coordinates the setup of servers, running
  tests, creating and resolving failures, and interpreting results."
  (:use     clojure.tools.logging)
  (:require [clojure.stacktrace :as trace]
            [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.generator :as generator]
            [jepsen.checker :as checker]
            [jepsen.client :as client]))

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
            (info "executing" op)

            ; Log invocation
            (swap! hist conj op)
            (recur
              (try
                ; Evaluate operation
                (let [completion (client/invoke! client test op)]
                  ; Sanity check
                  (info "Completion" completion)
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

(defn run!
  "Runs a test. Tests are maps containing

  :nodes      A sequence of string node names involved in the test.
  :ssh        SSH credential information: a map containing...
    :user     The username to connect with   (root)
    :key      A path to an SSH identity file (~/.ssh/id_rsa)
  :os         The operating system; given by the OS protocol
  :db         The database to configure: given by the DB protocol
  :client     A client for the database
  :nemesis    A client for failures
  :generator  A generator of operations to apply to the DB
  :model      The model used to verify the history is correct
  :checker    Verifies that the history is valid

  Tests proceed like so:

  1. Setup the operating system
  2. Setup the database
  2. Create the nemesis
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
  (let [test (assoc test :history (atom []))]

    ; Setup
    (dorun (pmap (partial os/setup! (:os test) test) (:nodes test)))
    (dorun (pmap (partial db/setup! (:db test) test) (:nodes test)))

    (let [nemesis (client/setup! (:nemesis test) test nil)
          clients (mapv (partial client/setup! (:client test) test)
                        (:nodes test))
          workers (mapv (partial worker test)
                        (cons :nemesis (iterate inc 0)) ; Process IDs
                        (cons nemesis clients))]        ; Clients

      ; Wait for workers to complete
      (dorun (map deref workers))

      ; Teardown
      (dorun (pmap (partial db/teardown! (:db test) test) (:nodes test)))
      (dorun (pmap (partial os/teardown! (:os test) test) (:nodes test)))

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
        (assoc test :results results)))))
