(ns yugabyte.client
  "Helper functions for working with Cassaforte clients."
  (:require [clojurewerkz.cassaforte [client :as c]
                                     [query :as q]
                                     [policies :as policies]
                                     [cql :as cql]]
            [clojure.tools.logging :refer [info]]
            [dom-top.core :as dt])
  (:import (com.datastax.driver.core.exceptions DriverException
                                                UnavailableException
                                                OperationTimedOutException
                                                ReadTimeoutException
                                                WriteTimeoutException
                                                NoHostAvailableException
                                                TransportException)))

(defn connect
  "Opens a new client, with helpful defaults for YugaByte. Options are passed to
  Cassaforte."
  ([node]
   (connect node {}))
  ([node opts]
   (c/connect [node]
              (merge
                {:protocol-version 3
                 :retry-policy    (policies/retry-policy
                                    :no-retry-on-client-timeout)}
                opts))))

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
     client/Client
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
  (let [conn (connect node)]
    (dt/with-retry [tries 3]
      (when (< 0 tries 3)
        (info "Waiting for cluster setup")
        (Thread/sleep 1000))

      (when (zero? tries)
        (throw (RuntimeException. "Gave up waiting for cluster setup.")))

      ; We need to do this serially to avoid a race in table creation
      (locking await-setup
        ; This... doesn't actually seem to guarantee that subsequent attempts
        ; to create keyspaces, tables, and rows will work. Grrr.
        (cql/create-keyspace conn "jepsen_setup"
                             (q/if-not-exists)
                             (q/with
                               {:replication
                                {"class"              "SimpleStrategy"
                                 "replication_factor" 3}}))
        (c/execute conn (str "CREATE TABLE IF NOT EXISTS jepsen_setup.waiting"
                             " (id INT PRIMARY KEY)"
                             " WITH transactions = { 'enabled': true }"))
        (cql/insert-with-ks conn "jepsen_setup" "waiting"
                            {:id 0}))
      (info "Cluster ready")

      (catch com.datastax.driver.core.exceptions.InvalidQueryException e
        (condp re-find (.getMessage e)
          #"SQL error: Invalid Table Definition. Invalid argument: Error creating table .+? num_tablets should be greater than 0. Client would need to wait for master leader get heartbeats from tserver"
          (retry (dec tries))

          (throw e)))

      (finally
        (c/disconnect! conn)))))
