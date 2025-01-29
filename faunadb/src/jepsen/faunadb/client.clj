(ns jepsen.faunadb.client
  "A clojure client for FaunaDB"
  (:import (com.faunadb.client FaunaClient)
           (com.faunadb.client.types Codec
                                     Decoder
                                     Field
                                     Value
                                     Value$ObjectV
                                     Value$ArrayV
                                     Value$RefV
                                     Value$LongV
                                     Value$DoubleV
                                     Value$TimeV
                                     Value$StringV
                                     Value$BooleanV
                                     Types)
           (com.faunadb.client.errors UnavailableException)
           (com.faunadb.client.query Language
                                     Expr)
           (com.faunadb.client.query Fn$Unescaped
                                     Fn$UnescapedObject
                                     Fn$UnescapedArray)
           (com.fasterxml.jackson.databind.node NullNode)
           (java.io IOException)
           (java.time Instant))
  (:require [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [warn info]]
            [slingshot.slingshot :refer [try+ throw+]]
            [dom-top.core :as dt]
            [wall.hack]
            [jepsen.util :as util]
            [jepsen.faunadb.query :as q]))

(def root-key
  "Administrative key for the FaunaDB cluster."
  "secret")

(def BoolField
  (Field/as Codec/BOOLEAN))

(def LongField
  (Field/as Codec/LONG))

(defn client
  "Constructs a Fauna client for the given node. If a path, like
  \"/linearized\", is provided, makes requests to that path instead of /."
  ([node]
   (client node ""))
  ([node path]
   (.build
     (doto (FaunaClient/builder)
       (.withEndpoint (str "http://" node ":8443" path))
       (.withSecret root-key)))))

(defn linearized-client
  "Constructs a Fauna client for the /linearized endpoint"
  [node]
  (client node "/linearized"))

(defn expr->json
  "Massage expressions back into something approximating json"
  [x]
  (if-not (instance? Expr x)
    x
    (let [j (wall.hack/method com.faunadb.client.query.Expr
                                :toJson
                                []
                                x)]
      (condp instance? j
        java.util.Map   (->> j
                             (map (fn [[k v]] [k (expr->json v)]))
                             (into (sorted-map)))
        java.util.List  (mapv (fn [v] (expr->json v)) j)
        NullNode        nil
        String          j
        Long            j
        (do (info "Don't know how to translate" (class j))
            j)))))

(defn json->data
  "Turn JSON-style structs into a human-readable AST"
  [x]
  (cond
    (map? x)
    (cond
      (get x "at")      (list 'at (json->data (get x "at"))
                              (json->data (get x "expr")))
      (get x "do")      (cons 'do (json->data (get x "do")))
      (get x "exists")  (list 'exists (json->data (get x "exists")))
      (get x "get")     (list 'get (json->data (get x "get")))
      (get x "if")      (list 'if (json->data (get x "if"))
                              (json->data (get x "then"))
                              (json->data (get x "else")))
      (get x "let")     (list 'let (json->data (get x "let"))
                              (json->data (get x "in")))
      (get x "not")     (list 'not (json->data (get x "not")))
      (get x "object")  (util/map-vals json->data (get x "object"))
      (get x "select")  (list 'select (json->data (get x "select"))
                              (json->data (get x "from")))
      (get x "time")    (list 'time (json->data (get x "time")))
      (get x "var")     (list 'var (json->data (get x "var")))
      true              x)

    (vector? x) (mapv json->data x)
    true        x))

(defn expr->data
  "Massage expressions back into something approximating json."
  [x]
  (-> x expr->json json->data))

(defrecord Ref [db class id])

(defn decode
  "Takes a Fauna value and converts it to a nice Clojure value."
  [^Value x]
  (when x
    (condp instance? x
      Value$ObjectV (->> (.get (Decoder/decode x (Types/hashMapOf Value)))
                         (reduce (fn [m [k v]]
                                   (assoc! m (keyword k) (decode v)))
                                 (transient {}))
                         (persistent!))

      Value$RefV    (Ref. (decode (.orElse (.getDatabase x) nil))
                          (decode (.orElse (.getClazz x) nil))
                          (.getId x))
      Value$ArrayV  (->> (.get (Decoder/decode x (Types/arrayListOf Value)))
                         (map decode))
      Value$LongV    (.get (Decoder/decode x Long))
      Value$DoubleV  (.get (Decoder/decode x Double))
      Value$BooleanV (.get (Decoder/decode x Boolean))
      Value$StringV  (.get (Decoder/decode x String))
      Value$TimeV    (.get (Decoder/decode x Instant))
      (do (info "Don't know how to decode" (class x) x)
          x))))

(def ^:dynamic *trace*
  "Flag for tracing"
  false)

(defmacro trace [& body]
  `(binding [*trace* true] ~@body))

(defn query*
  "Raw version of query; doesn't decode results."
  [conn e]
  (try
    ; Basically every exception thrown by the client is
    ; concurrentexecutionexception. This makes error handling awkward because
    ; you can't catch error types any more. We can work around that by throwing
    ; the cause, but then the stacktrace is just from the internal netty
    ; executor and tells us nothing about what code actually hit the exception.
    ; I don't have a good solution to this (pattern-matching catch expressions
    ; which can examine causes?).
    ;
    ; So... a compromise: for Jepsen's purposes we don't care much about Netty
    ; internals, so we're going to *replace* the original cause stacktrace with
    ; the CEE's stacktrace, which at least tells you where in Jepsen's code
    ; things went wrong.
    (let [r (.. conn (query (q/expr e)) (get))]
      (when *trace*
        (info "Query"
              (str/trimr (with-out-str (prn) (pprint (expr->data (q/expr e)))))
              (str/trimr (with-out-str (prn) (pprint (decode r))))))
      r)
    (catch java.util.concurrent.ExecutionException err
      (let [cause (.getCause err)]
        (when *trace*
          (info "Query"
                (str/trimr (with-out-str (prn) (pprint (expr->data (q/expr e)))))
                "\nThrew:" (.getClass cause) (.getMessage cause)))
        (.setStackTrace cause (.getStackTrace err))
        (throw cause)))))

(defn query
  "Performs a query on a connection, and returns results."
  [conn e]
  (decode (query* conn e)))

(defn now
  "Queries FaunaDB for the current time."
  [conn]
  (query conn (q/time "now")))

(defn query-all-naive
  "Performs a query for an expression. Paginates expression, performs query,
  and returns a lazy sequence of the :data from each page of results. This is
  the naive approach used by e.g. the JS drivers; not transactional.

  Options:

  :size   - Number of results per page"
  ([conn expr]
   (query-all-naive conn expr {}))
  ([conn expr opts]
   (lazy-seq
     ; If we don't have a time, we're going to wrap the expression in an array
     ; and include the current time; then we'll extract that and use it for
     ; future times. If we *do* have a time, then we'll use it as the time for
     ; the query.
     (let [expr          (q/expr expr)
           after         (:after opts)
           size          (:size opts)
           expr'         (q/paginate expr {:after after
                                           :size  size})
           page          (query* conn expr')
           after         (.at page (into-array String ["after"]))
           data          (:data (decode page))]
       (if (= after q/null)
         data
         (concat data (query-all-naive conn expr {:after after
                                                  :size  size})))))))

(defn query-all
  "Performs a query for an expression. Paginates results, performs query, and
  returns a lazy sequence of the :data from each page of results. This is a
  transactional variant which should be correctly isolated.

  Options:

  :at     - A timestamp for reads; if none is provided, will query at the
            current time, and re-use the same time for later pages.
  :size   - Number of results per page"
  ([conn expr]
   (query-all conn expr {}))
  ([conn expr opts]
   (lazy-seq
     ; If we don't have a time, we're going to wrap the expression in an array
     ; and include the current time; then we'll extract that and use it for
     ; future times. If we *do* have a time, then we'll use it as the time for
     ; the query.
     (let [expr          (q/expr expr)
           after         (:after opts)
           time          (:ts opts)
           size          (:size opts)
           expr'         (q/paginate expr {:after after
                                           :size  size})
           expr'         (if time
                          (q/at time expr')
                          [(q/time "now") expr'])
           res          (query* conn expr')
           [time page]  (if time
                          ; We did a plain paginated query
                          [time res]
                          ; We've got an array of [time, page]
                          (let [pair (.get (Decoder/decode
                                             res (Types/arrayListOf Value)))]
                            pair))
           after        (.at page (into-array String ["after"]))
           data         (:data (decode page))]
       (if (= after q/null)
         data
         (concat data (query-all conn expr {:after after
                                            :ts    time
                                            :size  size})))))))

(defn upsert-by-ref
  "Takes a ref and an instance map; constructs an expr which creates the ref
  with that data if it does not already exist, and updates it otherwise. TODO:
  write instead of update?"
  [r data]
  (q/if (q/exists? r)
    (q/update r data)
    (q/create r data)))

(defn maybe-insert
  "Takes a ref and an instance map; constructs an expr which creates the ref
  with that data iff it doesn't already exist, otherwise does nothing."
  [r data]
  (q/when (q/not (q/exists? r))
    (q/create r data)))

; Oh gosh, these shouldn't be named upsert, what was I thinking
(defn upsert-class
  "Query expr to upsert a class. Takes a class map."
  [cm]
  (q/when (q/not (q/exists? (q/class (:name cm))))
    (q/create-class cm)))

(defn upsert-index
  "Query expression to upsert an index. Takes an index map."
  [im]
  (q/when (q/not (q/exists? (q/index (:name im))))
    (q/create-index im)))

(defn upsert-class!
  "Class insertion is not transactional, and can throw \"Instance is not
  unique\" even when using upsert-class. We can automatically recover from
  this, but only when upserting a single class; e.g. we have to give up
  compositional queries. This function takes a conn and a class map, and
  performs the upsert."
  [conn cm]
  (try
    (query conn (upsert-class cm))
    (catch com.faunadb.client.errors.BadRequestException e
      (if (re-find #"instance not unique" (.getMessage e))
        :instance-not-unique
        (throw e)))))

(defn upsert-index!
  "Like upsert-class! for indices."
  [conn im]
  (try
    (query conn (upsert-index im))
    (catch com.faunadb.client.errors.BadRequestException e
      (if (re-find #"instance not unique" (.getMessage e))
        :instance-not-unique
        (throw e)))))

(defn jitter-time
  "Jitters an Instant timestamp by +/- jitter milliseconds (default 10 s)"
  ([t]
   (jitter-time t 10000))
  ([^Instant t jitter]
   (.plusMillis t (- (rand-int (* 2 jitter))
                     jitter))))

(defn maybe-at
  "Useful for comparing the results of regular queries to At(...) queries. This
  takes a test, used to determine whether to use an At query, a Fauna client,
  and a query expression. If (:at-query test) is true, rewrites the query to
  use a recent timestamp using (At expr). Otherwise, returns expr.

  In both cases, the shape of the query changes; instead of returning
  expr-results, it returns a tuple of [ts, expr-results]. If this test does not
  use at queries, ts will be `nil`.

  We have two methods for recent timestamps.

  - To test a very recent timestamp, we get the current time and bind it in a
  Let.

  - Another option is to perform a separate query for the current time, and
  then to make a second query with that timestamp. We apply a randomized
  jitter.

  We select between these methods at random."
  [test conn expr]
  (if-not (:at-query test)
    ; Default case, don't wrap in an `at`
    (q/expr [nil expr])

    (condp < (rand)
      ; Separate query
      0.5 (let [t (jitter-time (now conn))]
            [t (q/at t (q/expr expr))])

      ; Single query
      (q/let [internal-maybe-at-ts (q/time "now")]
        [internal-maybe-at-ts (q/at internal-maybe-at-ts
                                    (q/expr expr))]))))

(defmacro with-retry
  "Useful for setup; retries requests when the cluster is unavailable. I'm not
  convinced UnavailableException is actually a definite failure, so just to be
  safe you should probably make the body idempotent."
  [& body]
  `(dt/with-retry [tries# 5]
    ~@body
    (catch java.util.concurrent.TimeoutException e#
      (if (< 1 tries#)
        (do (info "Waiting for cluster ready")
            (Thread/sleep 5000)
            (~'retry (dec tries#)))
        (throw e#)))
    (catch com.faunadb.client.errors.UnavailableException e#
      (if (< 1 tries#)
        (do (info "Waiting for cluster ready")
            (Thread/sleep 5000)
            (~'retry (dec tries#)))
        (throw e#)))))

(defmacro with-errors
  "Takes an operation, a set of idempotent operation :fs, and a body. Evaluates
  body; catches common Fauna exceptions and maps them to appropriate :fail or
  :info results."
  [op idempotent & body]
  `(let [type# (if (~idempotent (:f ~op)) :fail :info)]
     (try
       ~@body
       (catch UnavailableException e#
         (assoc ~op :type type#, :error [:unavailable (.getMessage e#)]))

       (catch java.net.ConnectException e#
         (Thread/sleep 1000) ; We likely won't be able to reconnect quickly, so
                             ; take a breather here
         (assoc ~op :type :fail, :error [:connect (.getMessage e#)]))

       (catch java.util.concurrent.TimeoutException e#
         (assoc ~op :type type#, :error [:timeout (.getMessage e#)]))

       (catch IOException e#
         (condp re-find (.getMessage e#)
           #"Connection refused"
           (do (Thread/sleep 1000) ; Chances are we're not gonna be able to
                                   ; reconnect quickly, so let's slow down
               (assoc ~op :type :fail, :error :connection-refused))
           (assoc ~op :type type#, :error [:io (.getMessage e#)])))

       (catch com.faunadb.client.errors.InternalException e#
         (condp re-find (.getMessage e#)
           #"fauna\.repo\.UninitializedException"
           (assoc ~op :type :fail, :error :repo-uninitialized)

           #"Transaction Coordinator is shut down"
           (assoc ~op :type :fail, :error :transaction-coordinator-shut-down)

           (assoc ~op
                  :type type#
                  :error [:internal-exception (.getMessage e#)])))

       (catch com.faunadb.client.errors.UnknownException e#
         (condp re-find (.getMessage e#)
           #"operator error: No configured replica for key"
           (assoc ~op :type :fail, :error :no-configured-replica))))))

(defn wait-for-index
  "Waits for the `active` flag on the given index ref. Times out after 2000
  seconds by default. Timeout is in milliseconds. Concurrent calls with the
  same index name block, to avoid having a bunch of clients spam the DB polling
  for activation."
  ([conn index]
   (wait-for-index conn index 2000000)) ; oh my god how does this take so long
  ([conn index timeout]
   ; this is probably bad and I should probably feel bad, but the interning/GC
   ; logic is *right there*...
   (locking (.intern (str "jepsen.faunadb.query/wait-for-index/"
                          (pr-str (expr->data index))))
     (util/timeout timeout
                   (throw+ {:type :timeout})
                   (loop []
                     (let [res (query conn (q/get index))]
                       (if (:active res)
                         nil
                         (do
                           (info "Waiting for index" (expr->data index))
                             ; (str "\n" (with-out-str (pprint res))))
                           (Thread/sleep 10000)
                           (recur)))))))))
