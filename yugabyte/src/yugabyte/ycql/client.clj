(ns yugabyte.ycql.client
  "Helper functions for working with Cassaforte clients."
  (:require [clojurewerkz.cassaforte [client :as c]
                                     [query :as q]
                                     [policies :as policies]
                                     [cql :as cql]]
            [clojure.tools.logging :refer [info]]
            [clojure.pprint :refer [pprint]]
            [jepsen [util :as util]]
            [jepsen.control.net :as cn]
            [dom-top.core :as dt]
            [wall.hack :as wh]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.net InetSocketAddress)
           (com.datastax.driver.core Cluster
                                     Cluster$Builder
                                     HostDistance
                                     NettyOptions
                                     NettyUtil
                                     PoolingOptions
                                     ProtocolVersion
                                     SocketOptions
                                     ThreadingOptions)
           (com.datastax.driver.core.policies RoundRobinPolicy
                                              WhiteListPolicy)
           (com.datastax.driver.core.exceptions DriverException
                                                InvalidQueryException
                                                UnavailableException
                                                OperationTimedOutException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                NoHostAvailableException
                                                TransportException)
           (io.netty.channel.nio NioEventLoopGroup)
           (java.util.concurrent LinkedBlockingQueue
                                 ThreadPoolExecutor
                                 TimeUnit)))

(defmacro with-retry
  "Retries CQL unavailable/timeout errors for up to 120 seconds. Helpful for
  setting up initial data; YugaByte loves to throw 10+ second latencies at us
  early in the test."
  [& body]
  `(let [deadline# (+ (util/linear-time-nanos) (util/secs->nanos 120))
         sleep#    100] ; ms
     (dt/with-retry []
       ~@body
       (catch NoHostAvailableException e#
         (if (< deadline# (util/linear-time-nanos))
           (throw e#)
           (do (info "Timed out, retrying")
               (Thread/sleep (rand-int sleep#))
               (~'retry))))
       (catch OperationTimedOutException e#
         (if (< deadline# (util/linear-time-nanos))
           (throw e#)
           (do (info "Timed out, retrying")
               (Thread/sleep (rand-int sleep#))
               (~'retry)))))))

(defn epoll-event-loop-group-constructor
  "Why is this not public?"
  []
  (wh/field NettyUtil :EPOLL_EVENT_LOOP_GROUP_CONSTRUCTOR NettyUtil))

(defn epoll-available?
  "Annnd security policy prevents us from calling this ??!!? so uhhh, wall-hack
  our way in there too, sigh"
  []
  ; Also this returns a boolean which is NOT the usual Boolean/FALSE, so we
  ; coerce it to a normal one with (boolean)... don't even ask
  (boolean (wh/method NettyUtil :isEpollAvailable [] nil)))

(defn ^Cluster cluster
  "Constructs a Cassandra client Cluster object with appropriate options.
  Normally we'd let Cassaforte do this, but we need to reach into its guts to
  pass some options, like ThreadingOptions, Cassaforte doesn't support."
  [node]
  (.. (Cluster/builder)
    (withProtocolVersion (ProtocolVersion/fromInt 3))
    (withPoolingOptions (doto (PoolingOptions.)
                          (.setCoreConnectionsPerHost HostDistance/LOCAL 1)
                          (.setMaxConnectionsPerHost  HostDistance/LOCAL 1)))
    ; This is sort of a hack; we're allowed to call cn/ip here without an SSH
    ; connection because it memoizes, and we already called it during setup.
    (addContactPoint (cn/ip node))
    (withRetryPolicy (policies/retry-policy :no-retry-on-client-timeout))
    (withReconnectionPolicy (policies/constant-reconnection-policy 1000))
    (withSocketOptions (.. (SocketOptions.)
                         (setConnectTimeoutMillis 1000)
                         (setReadTimeoutMillis 5000)))
    (withLoadBalancingPolicy (WhiteListPolicy.
                               (RoundRobinPolicy.)
                               ; Same story: memoized.
                               [(InetSocketAddress. (cn/ip node) 9042)]))
    (withThreadingOptions (proxy [ThreadingOptions] []
                            (createExecutor [cluster-name]
                              (doto (ThreadPoolExecutor.
                                      1 ; Core pool size
                                      1 ; Max pool size
                                      30 ; How long to keep threads alive
                                      TimeUnit/SECONDS
                                      (LinkedBlockingQueue.)
                                      (.createThreadFactory
                                        this cluster-name "worker"))
                                (.allowCoreThreadTimeOut true)))))
    (withNettyOptions (proxy [NettyOptions] []
                        (eventLoopGroup [thread-factory]
                          (if (epoll-available?)
                            (.newInstance (epoll-event-loop-group-constructor)
                                          1 thread-factory)
                            (NioEventLoopGroup. 1 thread-factory)))))
    (build)))

(defn connect
  "Opens a new client, with helpful defaults for YugaByte. Options are passed to
  Cassaforte."
  ([node]
   (connect node {}))
  ([node opts]
   (with-retry
     (let [c (cluster node)]
       (try (.connect c)
            (catch DriverException e
              (.close c)
              (throw e)))))))

(defn execute-with-timeout!
  "Executes a statement on a session, but applies a custom read timeout, in
  milliseconds, for it."
  [session timeout statement]
  (let [statement (.. (c/build-statement statement)
                      (setReadTimeoutMillis timeout))]
    (c/execute session statement)))

(defn statement->str
  "Converts a statement to a string so we can do string munging on it."
  [s]
  (if (string? s)
    s
    (-> s
        ;(.setForceNoValues true)
        .getQueryString)))

(defn with-transactions
  "Takes a CQL create-table statement, converts it to a string, and adds \"WITH
  transctions = { 'enabled' : true }\". Sort of a hack, the Cassandra client
  doesn't know about this syntax, I think."
  [s]
  (str (statement->str s) " WITH transactions = { 'enabled' : true }"))

(defn create-table
  "Table creation is fairly slow in YB, so we need to run it with a custom
  timeout. Works just like cql/create-table."
  [conn & table-args]
  (execute-with-timeout! conn 30000 (apply q/create-table table-args)))

(defn create-index
  "Index creation is also slow in YB, so we run it with a custom timeout. Works
  just like cql/create-index, or you can pass a string if you need to use YB
  custom syntax.

  Also you, like, literally *can't* tell Cassaforte (or maybe Cassandra's
  client or CQL or YB?) to create an index if it doesn't exist, so we're
  swallowing the duplicate table execeptions here.
  TODO: update YB Cassaforte fork, so we can use `CREATE INDEX IF NOT EXISTS`.
  "
  [conn & index-args]
  (let [statement (if (and (= 1 (count index-args))
                           (string? (first index-args)))
                    (first index-args)
                    (apply q/create-index index-args))]
    (try (execute-with-timeout! conn 30000 statement)
         (catch InvalidQueryException e
           (if (re-find #"already exists" (.getMessage e))
             :already-exists
             (throw e))))))

(defn create-transactional-table
  "Like create-table, but enables transactions."
  [conn & table-args]
  (execute-with-timeout! conn 30000
                         (with-transactions
                           (apply q/create-table table-args))))

(defn ensure-keyspace!
  "Creates a keyspace using the given connection, if it doesn't already exist.
  Replication-factor is derived from the test."
  [conn keyspace-name test]
  (cql/create-keyspace conn keyspace-name
                       (q/if-not-exists)
                       (q/with {:replication
                                {"class" "SimpleStrategy"
                                 "replication_factor"
                                 (:replication-factor test)}})))

(defmacro with-errors
  "Takes an op, a set of idempotent operation :fs, and a body. Evalates body,
  and catches common errors, returning an appropriate completion for `op`."
  [op idempotent & body]
  `(let [crash# (if (~idempotent (:f ~op)) :fail :info)]
     (try
       ~@body
       (catch UnavailableException e#
         ; I think this was used back when we blocked on all nodes being online
         ; (info "Not enough replicas - failing")
         (assoc ~op :type :fail, :error [:unavailable (.getMessage e#)]))

       (catch WriteTimeoutException e#
         (assoc ~op :type crash#, :error :write-timed-out))

       (catch ReadTimeoutException e#
         (assoc ~op :type crash#, :error :read-timed-out))

       (catch OperationTimedOutException e#
         (assoc ~op :type crash#, :error :operation-timed-out))

       (catch TransportException e#
         (condp re-find (.getMessage e#)
           #"Connection has been closed"
           (assoc ~op :type crash#, :error :connection-closed)

           (throw e#)))

       (catch NoHostAvailableException e#
         (condp re-find (.getMessage e#)
           #"no host was tried"
           (do (info "All nodes are down - sleeping 2s")
               (Thread/sleep 2000)
               (assoc ~op :type :fail :error [:no-host-available (.getMessage e#)]))
           (assoc ~op :type crash#, :error [:no-host-available (.getMessage e#)])))

       (catch DriverException e#
         (if (re-find #"Value write after transaction start|Conflicts with higher priority transaction|Conflicts with committed transaction|Operation expired: Failed UpdateTransaction.* status: COMMITTED .*: Transaction expired"
                      (.getMessage e#))
           ; Definitely failed
           (assoc ~op :type :fail, :error (.getMessage e#))
           (throw e#)))

       (catch InvalidQueryException e#
         ; This can actually mean timeout
         (if (re-find #"RPC to .+ timed out after " (.getMessage e#))
           (assoc ~op :type crash#, :error [:rpc-timed-out (.getMessage e#)])
           (throw e#))))))

(defmacro defclient
  "Helper for defining CQL clients. Takes a class name, a string keyspace, a
  vector of state fields (as for defrecord), followed by protocols and
  functions, like defrecord. Appends two fields, `conn`, and `keyspace-created`
  to the state fields, which stores the cassandra client connection, provides
  default open! and close! functions, and passes the whole state to defrecord.

  Defines a constructor without conn or keyspace-created fields. Args are 1:1
  with your state fields.

    (->MyClient)

  Automatically creates keyspace during setup!, and ensures that keyspace is
  used on every conn thereafter. We do this because CQL assumes clients set the
  keyspace once, and we don't want to do multiple network trips to set the
  keyspace for every operation.

  Calls to setup! take a lock to prevent concurrent creation of tables, which
  hits a bug in Yugabyte.

  Example:

    (c/defclient CQLBank []
      (setup! [this test]
        (do-stuff-with conn))

      (invoke! [this test op]
        ...)

      (teardown! [this test]))"
  [name keyspace fields & exprs]
  (let [[interfaces methods opts] (#'clojure.core/parse-opts+specs
                                    (cons 'jepsen.client/Client exprs))
        ; We're going to rewrite the setup! fn to lock, create the keyspace,
        ; and use it before executing user code.
        setup-code (->> methods
                        (filter (comp #{'setup!} first))
                        first
                        (drop 2))

        ; Strip the original setup from the interface list. This is a hack, we
        ; should handle multiple setup! fns from diff protocols correctly.
        exprs (->> (remove (fn [expr]
                             (and (list? expr)
                                  (= 'setup! (first expr))))
                           exprs))]
    `(do (defrecord ~name ~(conj (vec fields) 'conn 'keyspace-created)
           jepsen.client/Client
           (open! [~'this ~'test ~'node]
             (let [conn# (connect ~'node)]
               (when (realized? ~'keyspace-created)
                 (cql/use-keyspace conn# ~keyspace))
               (assoc ~'this :conn conn#)))

           (setup! [~'this ~'test]
             (locking ~'keyspace-created
               (ensure-keyspace! ~'conn ~keyspace ~'test)
               (deliver ~'keyspace-created true)
               (cql/use-keyspace ~'conn ~keyspace)
               ~@setup-code))

           (close! [~'this ~'test]
             (c/disconnect! ~'conn))

           ~@exprs)

         ; Constructor
         (defn ~(symbol (str "->" name))
           ~(vec fields)
           ; Pass user fields, conn, keyspace-created
           (new ~name ~@fields nil (promise))))))

(defn await-setup
  "Used at the start of a test. Takes a node, opens a connection to it, and
  evalulates some basic commands to make sure the cluster is ready to accept
  requests. Retries when necessary."
  [node]
  (let [max-tries 1000]
    (dt/with-retry [tries max-tries]
      (when (< 0 tries max-tries)
        (Thread/sleep 1000))

      (when (zero? tries)
        (info "Zero?, tries " tries)
        (throw (RuntimeException.
                 "Client gave up waiting for cluster setup.")))

      (let [conn (connect node)]
        (try
          ; We need to do this serially to avoid a race in table creation
          (locking await-setup
            ; This... doesn't actually seem to guarantee that subsequent
            ; attempts to create keyspaces, tables, and rows will work. Grrr.
            (cql/create-keyspace conn "jepsen_setup"
                                 (q/if-not-exists)
                                 (q/with
                                   {:replication
                                    {"class"              "SimpleStrategy"
                                     "replication_factor" 3}}))

            (execute-with-timeout!
              conn 10000
              (str "CREATE TABLE IF NOT EXISTS jepsen_setup.waiting"
                   " (id INT PRIMARY KEY, balance BIGINT)"
                   " WITH transactions = { 'enabled': true }"))

            (cql/insert-with-ks conn "jepsen_setup" "waiting"
                                {:id 0, :balance 5}))
          (info "Cluster ready")

          (finally
            (c/disconnect! conn))))

      (catch com.datastax.driver.core.exceptions.InvalidQueryException e
        (condp re-find (.getMessage e)
          #"num_tablets should be greater than 0"
          (do (info "Waiting for cluster setup: num_tablets was 0")
              (retry (dec tries)))

          #"Not enough live tablet servers to create table with replication factor"
          (do (info "Waiting for cluster setup: Not enough live tablet servers")
              (retry (dec tries)))

          (throw e)))

      (catch OperationTimedOutException e
        (info "Waiting for cluster setup: Timed out")
        (retry (dec tries)))

      (catch NoHostAvailableException e
        (info "Waiting for cluster setup: No host available")
        (retry (dec tries))))))
