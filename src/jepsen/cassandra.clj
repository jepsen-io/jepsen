(ns jepsen.cassandra
  "Cassandra test."
  (:import (com.datastax.driver.core ConsistencyLevel)
           (com.datastax.driver.core.exceptions NoHostAvailableException
                                                WriteTimeoutException
                                                ReadTimeoutException))
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.multi.cql :as cql]
            qbits.hayt.cql
            [jepsen.control         :as control]
            [jepsen.control.net     :as control.net]
            [jepsen.failure         :as failure]
            [jepsen.codec           :as codec]
            [clojure.set            :as set])
  (:use
    clojurewerkz.cassaforte.query
    jepsen.util
    jepsen.set-app
    jepsen.load))

(def keyspace "jepsen_keyspace")

(defn drop-keyspace
  "Nukes the keyspace."
  [session]
  (try
    (cql/drop-keyspace session keyspace)
    (catch com.datastax.driver.core.exceptions.InvalidQueryException e
      nil)))

(defn ensure-keyspace
  "Make sure the jepsen keyspace exists, and switches the client to it."
  [session]
  (cql/create-keyspace session keyspace
                       (with {:replication {:class "SimpleStrategy"
                                            :replication_factor 3}}))
  (cql/use-keyspace session keyspace))

(defn nodetool [node & args]
  (control/on node (apply control/exec :nodetool args)))


(defn cassandra-app
  "Tests linearizability on a cell by reading/writing opaque blobs."
  [opts]
  (let [table    "set_app"
        clock-skew (rand-int 100)
        cluster (client/build-cluster {:contact-points [(:host opts)]
                                       :port 9042})
        session (client/connect cluster)]
    (reify SetApp
      (setup [app]
        (teardown app)
        (ensure-keyspace session)
        (cql/create-table session table
                          (column-definitions {:id :int
                                               :elements :blob
                                               :primary-key [:id]}))
        (cql/insert session table
                    {:id 0 :elements (codec/encode [])}
                    (using :timestamp (+ (System/currentTimeMillis)
                                         clock-skew))))

      (add [app element]
        (client/with-consistency-level ConsistencyLevel/QUORUM
          (let [value (-> (cql/select session table (where :id 0))
                          first
                          :elements
                          codec/decode
                          (conj element)
                          codec/encode)]
            (cql/update session table
                        {:elements value}
                        (where :id 0)
                        (using :timestamp (+ (System/currentTimeMillis)
                                             clock-skew)))))
          ok)

      (results [app]
        (client/with-consistency-level ConsistencyLevel/ALL
          (->> (cql/select session table
                           (where :id 0))
               first
               :elements
               codec/decode)))

      (teardown [app]
        (drop-keyspace session)))))

(defn counter-app
  "All writes are increments. Recovers [0...n] where n is the current value of
  the counter."
  [opts]
  (let [table   "counter_app"
        cluster (client/build-cluster {:contact-points [(:host opts)]
                                       :port 9042})
        session (client/connect cluster)]
    (reify SetApp
      (setup [app]
        (teardown app)
        (ensure-keyspace session)
        (cql/create-table session table
                          (column-definitions {:id :int
                                               :count :counter
                                               :primary-key [:id]})))
      
      (add [app element]
        (client/with-consistency-level ConsistencyLevel/ONE
          (cql/update session table
                      {:count (increment-by 1)}
                      (where :id 0)))
        ok)

      (results [app]
        (client/with-consistency-level ConsistencyLevel/ALL
          (->> (cql/select session table (where :id 0))
               first
               :count
               range)))

      (teardown [app]
        (drop-keyspace session)))))

(defn set-app
  "Uses CQL sets"
  [opts]
  (let [table "set_app"
        cluster (client/build-cluster {:contact-points [(:host opts)]
                                       :port 9042})
        session (client/connect cluster)]
    (reify SetApp
      (setup [app]
        (teardown app)
        (ensure-keyspace session)
        (cql/create-table session table
                          (column-definitions {:id :int
                                               :elements (set-type :int)
                                               :primary-key [:id]}))
        (cql/insert session table
                    {:id 0
                     :elements #{}}))

      (add [app element]
        (client/with-consistency-level ConsistencyLevel/ANY
          (cql/update session table
                      {:elements [+ #{element}]}
                      (where :id 0)))
        ok)
      
      (results [app]
        (client/with-consistency-level ConsistencyLevel/ALL
          (->> (cql/select session table (where :id 0))
               first
               :elements)))

      (teardown [app]
        (drop-keyspace session)))))

; Hack: use this to record the set of all written elements for isolation-app.
(def writes (atom #{}))

(defn isolation-app
  "This app tests whether or not it is possible to consistently update multiple
  cells in a row, such that either *both* writes are visible together, or
  *neither* is.

  Each client picks a random int identifier to distinguish itself from the
  other clients. It tries to write this identifier to cell A, and -identifier
  to cell B. The write is considered successful if A=-B. It is unsuccessful if
  A is *not* equal to -B; e.g. our updates were not isolated.
  
  'concurrency defines the number of writes made to each row. "
  [opts]
  (let [table        "isolation_app"
        ; Number of writes to each row
        concurrency  2
        ; Mean of uniformly distributed latency for writes
        mean-latency 100
        client-id   (rand-int Integer/MAX_VALUE)
        cluster     (client/build-cluster {:contact-points [(:host opts)]
                                           :port 9042})
        session     (client/connect cluster)]
    (reify SetApp
      (setup [app]
        (teardown app)
        (ensure-keyspace session)
        (cql/create-table session table
                          (column-definitions {:id :int
                                               :a  :int
                                               :b  :int
                                               :primary-key [:id]})))

      (add [app element]
        ; Introduce some entropy
        (sleep (rand 200))

        ; Record write in memory
        (swap! writes conj element)

        ; Write to Cassy
        (client/with-consistency-level ConsistencyLevel/ANY
          (dotimes [i concurrency]
            (let [e (- element i)]
              (when (<= 0 e)
                (client/execute
                  session
                  (->> (insert-query table
                                     {:id e
                                      :a client-id
                                      :b (- client-id)})
                       ; If you force timestamp collisions instead of letting
                       ; them happen naturally, you can reliably cause
                       ; conflicts in 99% of rows! :D
                       ; (using :timestamp 1)
                       queries
                       batch-query
                       client/render-query)
                  :prepared qbits.hayt.cql/*prepared-statement*)))))
        ok)

      (results [app]
        (client/with-consistency-level ConsistencyLevel/ALL
          (->> (cql/select session table)
               (remove #(= (:a %) (- (:b %))))
               prn
               dorun)

          (->> (cql/select session table)
               (remove #(= (:a %) (- (:b %))))
               (map :id)
               (set/difference @writes))))

      (teardown [app]
        (drop-keyspace session)))))

(defn txn-success?
  "Was the given transaction result succesful?"
  [result]
  (boolean (get result (keyword "[applied]"))))

(defn transaction-app
  "Uses Paxos CAS"
  [opts]
  (let [table    "set_app"
        clock-skew (rand-int 100)
        cluster (client/build-cluster {:contact-points [(:host opts)]
                                       :port 9042})
        session (client/connect cluster)]
    (reify SetApp
      (setup [app]
        (teardown app)
        (ensure-keyspace session)
        (cql/create-table session table
                          (column-definitions {:id :int
                                               :elements :blob
                                               :primary-key [:id]}))
        (cql/insert session table
                    {:id 0 :elements (codec/encode [])}))

      (add [app element]
        (client/with-consistency-level ConsistencyLevel/QUORUM
          (let [t0 (System/currentTimeMillis)]
            (loop []
              (let [res (try
                          (let [value (-> (cql/select session table
                                                      (where :id 0))
                                          first
                                          :elements)
                                value' (-> value
                                           codec/decode
                                           (conj element)
                                           codec/encode)]
                            (-> (cql/update session table
                                            {:elements value'}
                                            (where :id 0)
                                            (only-if {:elements value}))
                                first))
                          (catch ReadTimeoutException e :timeout)
                          (catch WriteTimeoutException e :timeout))]
                (cond
                  ; Successful write
                  (txn-success? res)
                  ok

                  ; Enough time to retry?
                  (< 10000 (- (System/currentTimeMillis) t0))
                  error

                  :else
                  (do
                    ;                      (log "retry" element)
                    (sleep (rand 100))
                    (recur))))))))

      (results [app]
        (client/with-consistency-level ConsistencyLevel/ALL
          (->> (cql/select session table
                           (where :id 0))
               first
               :elements
               codec/decode)))

      (teardown [app]
        (drop-keyspace session)))))

(def dupes (atom {}))

(defn transaction-dup-app
  "Tests that transactions may only succeed once."
  [opts]
  (let [table "transaction_dup_app"
        cluster (client/build-cluster {:contact-points ["n1" "n2" "n3" "n4" "n5"]
                                       :port 9042})
        session (client/connect cluster)]
    (reify SetApp
      (setup [app]
        (teardown app)
        (ensure-keyspace session)
        (cql/create-table session table
                          (column-definitions {:id :int
                                               :consumed :boolean
                                               :primary-key [:id]})))

      (add [app element]
        (client/with-consistency-level ConsistencyLevel/QUORUM
          (try
            ; Insert an unconsumed record
            (cql/insert session table
                        {:id element :consumed false})
            ; Consume once
            (let [r1 (cql/update session table
                                 {:consumed true}
                                 (where :id element)
                                 (only-if {:consumed false}))
                  ; Consume again
                  r2 (cql/update session table
                                 {:consumed true}
                                 (where :id element)
                                 (only-if {:consumed false}))]
              (if (and (txn-success? r1)
                       (txn-success? r2))
                (do
                  (log "Dupe found!" r1 r2)
                  (swap! dupes conj element)
                  ok)
                ok))
            (catch ReadTimeoutException e :timeout)
            (catch WriteTimeoutException e :timeout))))

      (results [app]
        (client/with-consistency-level ConsistencyLevel/ALL
          (->> (cql/select session table)
               (map :id)
               (remove @dupes))))

      (teardown [app]
        (reset! dupes #{})
        (drop-keyspace session)))))
