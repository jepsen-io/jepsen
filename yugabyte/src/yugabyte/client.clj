(ns yugabyte.client
  "Helper functions for working with Cassaforte clients."
  (:require [clojurewerkz.cassaforte [client :as c]
                                     [query :as q]
                                     [policies :as policies]
                                     [cql :as cql]]
            [clojure.tools.logging :refer [info]]
            [dom-top.core :as dt]
            [wall.hack :as wh])
  (:import (java.net InetSocketAddress)
           (com.datastax.driver.core Cluster
                                     Cluster$Builder
                                     HostDistance
                                     NettyOptions
                                     NettyUtil
                                     PoolingOptions
                                     ProtocolVersion
                                     ThreadingOptions)
           (com.datastax.driver.core.policies RoundRobinPolicy
                                              WhiteListPolicy)
           (com.datastax.driver.core.exceptions DriverException
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
    (addContactPoint node)
    (withRetryPolicy (policies/retry-policy :no-retry-on-client-timeout))
    (withReconnectionPolicy (policies/constant-reconnection-policy 1000))
    (withLoadBalancingPolicy (WhiteListPolicy.
                               (RoundRobinPolicy.)
                               [(InetSocketAddress. node 9042)]))
    (withThreadingOptions (proxy [ThreadingOptions] []
                            (createExecutor [cluster-name]
                              (info "Creating executor")
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
   (let [c (cluster node)]
     (try (.connect c)
          (catch DriverException e
            (.close cluster)
            (throw e))))))

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
         (info "All nodes are down - sleeping 2s")
         (Thread/sleep 2000)
         (assoc ~op :type :fail :error [:no-host-available (.getMessage e#)]))

       (catch DriverException e#
         (if (re-find #"Value write after transaction start|Conflicts with higher priority transaction|Conflicts with committed transaction|Operation expired: Failed UpdateTransaction.* status: COMMITTED .*: Transaction expired"
                      (.getMessage e#))
           ; Definitely failed
           (assoc ~op :type :fail, :error (.getMessage e#))
           (throw e#))))))

(defmacro defclient
  "Helper for defining CQL clients. Takes a class name, a vector of state
	fields (as for defrecord), followed by protocols and functions, like
	defrecord. Appends a field, `conn`, to the state fields, which stores the
  cassandra client connection, provides default open! and close! functions, and
  passes the whole state to defrecord.

  Example:

		(c/defclient CQLBank []
			(setup! [this test]
				(do-stuff-with conn))

			(invoke! [this test op]
        ...)

			(teardown! [this test]))"
  [name fields & exprs]
  `(defrecord ~name ~(conj (vec fields) 'conn)
     jepsen.client/Client
     (open! [~'this ~'test ~'node]
       (assoc ~'this :conn (connect ~'node)))

     (close! [~'this ~'test]
       (c/disconnect! ~'conn))

     ~@exprs))

(defn await-setup
  "Used at the start of a test. Takes a node, opens a connection to it, and
  evalulates some basic commands to make sure the cluster is ready to accept
  requests. Retries when necessary."
  [node]
  (let [max-tries 1000]
    (dt/with-retry [tries max-tries]
      (let [conn (connect node)]
        (try
          (when (< 0 tries max-tries)
            (Thread/sleep 1000))

          (when (zero? tries)
            (throw (RuntimeException.
                     "Client gave up waiting for cluster setup.")))

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
            (c/execute conn (str "CREATE TABLE IF NOT EXISTS jepsen_setup.waiting"
                                 " (id INT PRIMARY KEY, balance BIGINT)"
                                 " WITH transactions = { 'enabled': true }"))
            (cql/insert-with-ks conn "jepsen_setup" "waiting"
                                {:id 0, :balance 5}))
          (info "Cluster ready")

          (finally
            (c/disconnect! conn))))

      (catch com.datastax.driver.core.exceptions.InvalidQueryException e
        (condp re-find (.getMessage e)
          #"SQL error: Invalid Table Definition. Invalid argument: Error creating table .+? num_tablets should be greater than 0. Client would need to wait for master leader get heartbeats from tserver"
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
