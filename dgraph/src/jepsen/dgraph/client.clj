(ns jepsen.dgraph.client
  "Clojure wrapper for the Java DGraph client library. This whole thing is
  *riddled* with escaping vulnerabilities, so, you know, tread carefully."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [with-retry]]
            [wall.hack]
            [cheshire.core :as json]
            [jepsen.client :as jc]
            [jepsen.dgraph.trace :as t])
  (:import (java.util.concurrent TimeUnit)
           (com.google.protobuf ByteString)
           (io.grpc ManagedChannel
                    ManagedChannelBuilder)
           (io.dgraph DgraphGrpc
                      DgraphGrpc$DgraphStub
                      DgraphClient
                      DgraphAsyncClient
                      Transaction
                      DgraphProto$Assigned
                      DgraphProto$Mutation
                      DgraphProto$Response
                      DgraphProto$Operation
                      TxnConflictException)))

(def default-port "Default dgraph alpha GRPC port" 9080)

;; milliseconds given to the grpc blockingstub as a deadline
(def deadline 30000)

(defn open
  "Creates a new DgraphClient for the given node."
  ([node]
   (t/with-trace "client.open!"
     (open node default-port)))
  ([node port]
   (t/with-trace "client.open!"
     (let [channel (.. (ManagedChannelBuilder/forAddress node port)
                       (usePlaintext true)
                       (build))
           stub  (DgraphGrpc/newStub channel)
           stub  (.withDeadlineAfter stub deadline TimeUnit/MILLISECONDS)
           stubs (into-array DgraphGrpc$DgraphStub [stub])]
       (DgraphClient. stubs)))))

(defn close!
  "Closes a client. Close is asynchronous; resources may be freed some time
  after calling (close! client)."
  [client]
  (t/with-trace "client.close!"
    (let [async-client (wall.hack/field DgraphClient :asyncClient client)]
      (doseq [c (wall.hack/field DgraphAsyncClient :stubs async-client)]
        (.. c getChannel shutdown)))))

(defn abort-txn!
  "Aborts a transaction object."
  [^Transaction t]
  (t/with-trace "client.abort-txn!"
    (try (.discard t)
       (catch io.grpc.StatusRuntimeException e
         (if (re-find #"ABORTED: Transaction has been aborted\. Please retry\."
                      (.getMessage e))
           :aborted
           (throw e))))))

(defmacro with-txn
  "Takes a vector of a symbol and a client. Opens a transaction on the client,
  binds it to that symbol, and evaluates body. Calls commit at the end of
  the body, or discards the transaction if an exception is thrown.

  If you commit or abort the transaction *within* body (e.g. before with-txn
  commits it for you), with-txn will attempt to commit, *not* throw, and return
  the result of `body`."
  [[txn-sym client] & body]
  `(let [~txn-sym (.newTransaction ^DgraphClient ~client)]
     (try
       (let [res# (do ~@body)]
         (try
           (.commit ~txn-sym)
           ;; If the user manually committed or aborted, that's OK.
           (catch io.dgraph.TxnFinishedException e#))
         res#)
       (finally
         (.discard ~txn-sym)))))

(defmacro with-unavailable-backoff
  "Wraps an expression returning a completion operation; for selected failure
  modes, sleeps a random amount of time before returning, so we don't spin our
  wheels against a down system."
  [& body]
  `(let [res# (do ~@body)]
     (when (and (= :fail (:type res#))
                (#{:unavailable
                   :predicate-moving
                   :unhealthy-connection} (:error res#)))
       (Thread/sleep (rand-int 2000)))
     res#))

(defmacro with-conflict-as-fail
  "Takes an operation and a body. Evaluates body; if a transaction conflict is
  thrown, returns `op` with :type :fail, :error :conflict."
  [op & body]
  `(with-unavailable-backoff
     (try ~@body
          (catch io.grpc.StatusRuntimeException e#
            (condp re-find (.getMessage e#)
              #"DEADLINE_EXCEEDED:"
              (assoc ~op, :type :info, :error :timeout)

              #"context deadline exceeded"
              (assoc ~op, :type :info, :error :timeout)

              #"Conflicts with pending transaction. Please abort."
              (assoc ~op :type :fail, :error :conflict)

              #"readTs: \d+ less than minTs: \d+ for key:"
              (assoc ~op :type :fail, :error :old-timestamp)

              #"Predicate is being moved, please retry later"
              (assoc ~op :type :fail, :error :predicate-moving)

              #"Tablet isn't being served by this instance"
              (assoc ~op :type :fail, :error :tablet-not-served-by-instance)

              #"Request sent to wrong server"
              (assoc ~op :type :fail, :error :wrong-server)

              #"Please retry again, server is not ready to accept requests"
              (assoc ~op :type :fail, :error :not-ready-for-requests)

              #"UNAVAILABLE"
              (assoc ~op, :type :fail, :error :unavailable)

              #"No connection exists"
              (assoc ~op :type :fail, :error :no-connection)

              ; Guessssing this means it couldn't even open a conn but not sure
              ; This might be a fail???
              #"Unavailable desc = all SubConns are in TransientFailure"
              (assoc ~op :type :info, :error :unavailable-all-subconns-down)

              #"rpc error: code = Unavailable desc = transport is closing"
              (assoc ~op :type :info, :error :unavailable-transport-closing)

              #"dispatchTaskOverNetwork: while retrieving connection. error: Unhealthy connection"
              (assoc ~op :type :info, :error :unhealthy-connection)

              #"Only leader can decide to commit or abort"
              (assoc ~op :type :fail, :error :only-leader-can-commit)

              #"This server doesn't serve group id:"
              (assoc ~op :type :fail, :error :server-doesn't-serve-group)

              #"ABORTED"
              (assoc ~op :type :fail, :error :transaction-aborted)

              (throw e#)))

          (catch TxnConflictException e#
            (assoc ~op :type :fail, :error :conflict)))))

(defn str->byte-string
  "Converts a string to a protobuf bytestring."
  [s]
  (ByteString/copyFromUtf8 s))

(defn alter-schema!
  "Takes a schema string (or any number of strings) and applies that alteration
  to dgraph. Retries if DEADLINE_EXCEEDED, since dgraph likes to throw this for
  ??? reasons at the start of the test. Should be idempotent, so... hopefully
  we can retry, at least in this context?"
  [^DgraphClient client & schemata]
  (t/with-trace "client.alter-schema!"
    (with-retry [i 10]
      (.alter client (.. (DgraphProto$Operation/newBuilder)
                         (setSchema (str/join "\n" schemata))
                         build))

      (catch java.util.concurrent.CompletionException e
        (let [message (.getMessage e)]
          (if (and (< 0 i)
                   (or (re-find #"DEADLINE_EXCEEDED" message)
                       (re-find #"Pending transactions" message)
                       (re-find #"ABORTED" message)))
            (do
              (warn "alter-schema! failed due to retriable error, retrying...")
              (Thread/sleep (rand-int 5000))
              (retry (dec i)))
            (throw e)))))))

(defn ^DgraphProto$Assigned mutate!*
  "Takes a mutation object and applies it to a transaction. Returns an
  Assigned."
  [^Transaction txn mut]
  ;(info "Mutate:" mut)
  (.mutate txn (.. (DgraphProto$Mutation/newBuilder)
                   (setSetJson (str->byte-string (json/generate-string mut)))
                   build)))

(defn mutate!
  "Like mutate!*, but returns a map of key names to UID strings."
  [txn mut]
  (t/with-trace "client.mutate"
    (.getUidsMap (mutate!* txn mut))))

(defn ^DgraphProto$Assigned set-nquads!*
  "Takes a transaction and an n-quads string, and adds those set mutations to
  the transaction."
  [^Transaction txn nquads]
  (.mutate txn (.. (DgraphProto$Mutation/newBuilder)
                   (setSetNquads (str->byte-string nquads))
                   build)))

(defn set-nquads!
  "Like set-nquads!*, but returns a map of key names to UID strings."
  [txn nquads]
  (t/with-trace "client.set-nquads!"
    (.getUidsMap (set-nquads!* txn nquads))))

(defn check-str-or-map
  "If the given value is a string, wraps it in a map with the :uid field."
  [x]
  (if (string? x)
    {:uid x}
    x))

(defn delete!
  "Deletes a record. Can take either a map (treated as a JSON deletion), or a
  UID string, in which case every outbound edge for the given entity is
  deleted."
  [^Transaction txn str-or-map]
  (t/with-trace "client.delete!"
    (let [target (check-str-or-map str-or-map)]
      (.mutate txn (.. (DgraphProto$Mutation/newBuilder)
                       (setDeleteJson (-> target
                                          json/generate-string
                                          str->byte-string))
                       build)))))

(defn graphql-type
  "Takes an object and infers a type in the query language, e.g.

      \"4\" -> \"string\",
      4     -> \"int\""
  [x]
  (when-not x
    (throw (IllegalArgumentException.
             "Can't infer graphql+- type for `nil`; did you mean to pass a non-nil value instead?")))

  (condp instance? x
    Long    "int"
    Integer "int"
    String  "string"
    Boolean "bool"
    Double  "float"
    clojure.lang.BigInt "int"
    (throw (IllegalArgumentException.
             (str "Don't know graphql+- type of " (pr-str x))))))

(defn query*
  "Runs a query given a graphql+- query string, and a map of variables for the
  query. Variables can be a map of strings, keywords, or symbols to strings,
  keywords, or symbols; they're all coerced to their string names, and prefixed
  with $.

      query(txn \"query all($a: string) { all(func: eq(name, $a)) { uid } }\"
            {:a \"cat\"})"
  ([^Transaction txn query-str]
   (json/parse-string (.. txn (query query-str) getJson toStringUtf8)
                      true))
  ([^Transaction txn query vars]
   ;(info "Query (vars:" (pr-str vars) "):" query)
   (let [vars (->> vars
                   (map (fn [[k v]] [(str "$" (name k)) (str v)]))
                   (into {}))
         res (.queryWithVars txn query vars)]
     (json/parse-string (.. res getJson toStringUtf8) true))))

(defn query
  "Like query*, but automatically generates the top-level `query` block with
  query variables inferred from the vars map. Example:

      query(txn,
            \"{ all(func: eq(name, $a)) { uid } }\"
            {:a \"cat\"})"
  ([txn query-str]
   (t/with-trace "client.query"
     (query* txn query-str)))
  ([txn query-str vars]
   (t/with-trace "client.query"
     (query* txn
             (str "query all("
                  (->> vars
                       (map (fn [[k v]] (str "$" (name k) ": " (graphql-type v))))
                       (str/join ", "))
                  ") " query-str)
             vars))))

(defn schema
  "Retrieves the current schema as JSON"
  [txn]
  (query txn "schema {}"))

(defn await-ready
  "Blocks until the server is up and responding to requests, or throws. Returns
  client."
  [client]
  (with-retry [attempts 16]
    (with-txn [t client]
      (schema t))
    (catch io.grpc.StatusRuntimeException e
      (cond (<= attempts 1)
            (throw e)

            (and (.getCause e)
                 (instance? java.net.ConnectException
                            (.getCause (.getCause e))))
            (do (info "GRPC interface unavailable, retrying in 5 seconds")
                (Thread/sleep 5000)
                (retry (dec attempts)))

            (re-find #"server is not ready to accept requests"
                     (.getMessage e))
            (do (info "Server not ready, retrying in 5 seconds")
                (Thread/sleep 5000)
                (retry (dec attempts)))

            :else
            (throw e))))
  client)

(defn upsert!
  "Takes a transaction, a predicate, and a record map. If only one map is
  provided, it is used as the predicate. If no record exists for the given
  predicate, inserts the record map.

  Predicate can be a keyword, which is used as the primary key of the record.
  TODO: add more complex predicates.

  Returns nil if upsert did not take place. Returns mutation results otherwise."
  [t pred record]
  (t/with-trace "client.upsert!"
    (if-let [pred-value (get record pred)]
      (let [res (-> (query t (str "{\n"
                                  "  all(func: eq(" (name pred) ", $a)) {\n"
                                  "    uid\n"
                                  "  }\n"
                                  "}")
                           {:a pred-value}))]
        ;;(info "Query results:" res)
        (when (empty? (:all res))
          ;;(info "Inserting...")
          (mutate! t record)))

      (throw (IllegalArgumentException.
              (str "Record " (pr-str record) " has no value for "
                   (pr-str pred)))))))

(defn gen-pred
  "Generates a predicate for a key, given a count of keys, and a prefix."
  [prefix n k]
  (str prefix "_" (mod (hash k) n)))

(defn gen-preds
  "Given a key prefix and a number of keys, generates all predicate names that
  might be used."
  [prefix n]
  (->> (range n)
       (map (fn [i] (str prefix "_" i)))))

(defrecord TxnClient [opts conn]
  jc/Client
  (open! [this test node]
    (assoc this :conn (open node)))

  (setup! [this test]
    (let [keys (->> (gen-preds "key" (:key-predicate-count opts))
                    (map (fn [pred] (str pred ": int @index(int)"
                                         (when (:upsert-schema test) " @upsert")
                                         " .\n")))
                    str/join)
          vals (->> (gen-preds "val" (:value-predicate-count opts))
                    (map (fn [pred] (str pred ": int .\n")))
                    str/join)]
      (alter-schema! conn (str keys vals))))

  (invoke! [this test op]
    (with-conflict-as-fail op
      (with-txn [t conn]
        (->> (:value op)
             (reduce
              (fn [txn' [f k v :as micro-op]]
                (let [kp (gen-pred "key" (:key-predicate-count opts) k)
                      vp (gen-pred "val" (:value-predicate-count opts) k)]
                  (case f
                    :r
                    (let [res (query t (str "{ q(func: eq(" kp ", $key)) {\n"
                                            "  " vp "\n"
                                            "}}")
                                     {:key k})
                          reads (:q res)]
                      (conj txn' [f k (condp = (count reads)
                                        ; Not found
                                        0 nil
                                        ; Found
                                        1 (get (first reads)
                                               (keyword vp))
                                        ; Ummm
                                        (throw (RuntimeException.
                                                (str "Unexpected multiple results for key "
                                                     (pr-str k) ": "
                                                     (pr-str reads)))))]))

                                        ; TODO: we should be able to optimize this to do pure
                                        ; inserts and UID-direct writes without the upsert
                                        ; read-write cycle, at least when we know the state
                    :w (do (if (:blind-insert-on-write? opts)
                             (mutate! t {(keyword kp) k, (keyword vp) v})
                             (upsert! t (keyword kp)
                                      {(keyword kp) k, (keyword vp) v}))
                           (conj txn' micro-op)))))
              [])
             (assoc op :type :ok, :value)))))

  (teardown! [this test])

  (close! [this test]
    (close! conn)))

(defn txn-client
  "A client which can execute generic transcational workloads over arbitrary
  integer keys and values.

  Options:

    :blind-insert-on-write?   If true, don't do upserts; just insert on every
                              write. Only appropriate when you'll never write
                              the same thing twice.
    :value-predicate-count    How many predicates to stripe values over.
                              Default: 5.
    :key-predicate-count      How many predicates to stripe keys over. Default:
                              5."
  [opts]
  (TxnClient.
    (merge {:blind-insert-on-write? false
            :value-predicate-count  5
            :key-predicate-count    5}
           opts)
    nil))
