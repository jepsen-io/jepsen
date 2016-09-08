(ns jepsen.cockroach
  "Tests for CockroachDB"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.jdbc :as j]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen
             [core :as jepsen]
             [db :as db]
             [os :as os]
             [tests :as tests]
             [control :as c :refer [|]]
             [store :as store]
             [nemesis :as nemesis]
             [generator :as gen]
             [independent :as independent]
             [reconnect :as rc]
             [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.cockroach [nemesis :as cln]
                              [auto :as auto :refer [cockroach-user
                                                     cockroach
                                                     jdbc-mode
                                                     db-port
                                                     insecure
                                                     db-user
                                                     db-passwd
                                                     store-path
                                                     dbname
                                                     verlog
                                                     log-files
                                                     pcaplog]]]))

(import [java.net URLEncoder])

;; timeout for DB operations during tests
(def timeout-delay 10000) ; milliseconds

;; number of simultaneous clients
(def concurrency-factor 30)

;; Isolation level to use with test transactions.
(def isolation-level :serializable)

;; for secure mode
(def client-cert "certs/node.client.crt")
(def client-key "certs/node.client.pk8")
(def ca-cert "certs/ca.crt")

;; Postgres user and dbname for jdbc-mode = :pg-*
(def pg-user "kena") ; must already exist
(def pg-passwd "kena") ; must already exist
(def pg-dbname "mydb") ; must already exist

;;;;;;;;;;;;;;;;;;;; Database set-up and access functions  ;;;;;;;;;;;;;;;;;;;;;;;

(def ssl-settings
  (if insecure
    ""
    (str "?ssl=true"
         "&sslcert=" client-cert
         "&sslkey=" client-key
         "&sslrootcert=" ca-cert
         "&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory")))

(defn db-conn-spec
  "Assemble a JDBC connection specification for a given Jepsen node."
  [node]
  (merge {:classname    "org.postgresql.Driver"
          :subprotocol  "postgresql"}
         (case jdbc-mode
           :cdb-cluster
           {:subname     (str "//" (name node) ":" db-port "/" dbname
                              ssl-settings)
            :user        db-user
            :password    db-passwd}

           :cdb-local
           {:subname     (str "//localhost:" db-port "/" dbname ssl-settings)
            :user        db-user
            :password    db-passwd}

           :pg-local
           {:subname     (str "//localhost/" pg-dbname)
            :user        pg-user
            :password    pg-passwd})))

(defn close-conn
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (j/db-find-connection conn)]
    (.close c))
  (dissoc conn :connection))

(defn client
  "Constructs a network client for a node, and opens it"
  [node]
  (rc/open!
    (rc/wrapper
      {:name [:cockroach node]
       :open (fn open []
               (util/timeout 30000
                             (throw (RuntimeException.
                                      (str "Connection to " node " timed out")))
                             (let [spec (db-conn-spec node)
                                   conn (j/get-connection spec)
                                   spec' (j/add-connection spec conn)]
                               (assert spec')
                               spec')))
       :close close-conn
       :log? true})))

(defmacro with-conn
  "Like jepsen.reconnect/with-conn, but also asserts that the connection has
  not been closed. If it has, throws an ex-info with :type :conn-not-ready.
  Delays by 1 second to allow time for the DB to recover."
  [[c client] & body]
  `(rc/with-conn [~c ~client]
     (when (.isClosed (j/db-find-connection ~c))
       (Thread/sleep 1000)
       (throw (ex-info "Connection not yet ready."
                       {:type :conn-not-ready})))
     ~@body))

(defn db
  "Sets up and tears down CockroachDB."
  [opts]
  (reify db/DB
    (setup! [_ test node]
      (when (= node (jepsen/primary test))
        (store/with-out-file test "jepsen-version.txt"
          (meh (->> (sh "git" "describe" "--tags")
                    (:out)
                    (print)))))

      (when (= jdbc-mode :cdb-cluster)
        (auto/install! test node)
        (auto/reset-clock!)
        (jepsen/synchronize test)

        (c/sudo cockroach-user
                (when (= node (jepsen/primary test))
                  (auto/start! test node)
                  (Thread/sleep 10000))

                (jepsen/synchronize test)
                (auto/packet-capture! node)
                (auto/save-version! node)
                (when (not= node (jepsen/primary test))
                  (auto/join! test node))

                (jepsen/synchronize test)
                (when (= node (jepsen/primary test))
                  (Thread/sleep 2000)
                  (auto/set-replication-zone!  ".default"
                                              {:range_min_bytes 1024
                                               :range_max_bytes 1048576})
                  (info node "Creating database...")
                  (auto/csql! (str "create database " dbname))))

        (info node "Setup complete")))

    (teardown! [_ test node]
      (when (= jdbc-mode :cdb-cluster)
        (auto/reset-clock!)

        (c/su
          (auto/kill! test node)

          (info node "Erasing the store...")
          (c/exec :rm :-rf store-path)

          (info node "Stopping tcpdump...")
          (meh (c/exec :killall -9 :tcpdump))

          (info node "Clearing the logs...")
          (doseq [f log-files]
            (when (cu/exists? f)
              (c/exec :truncate :-c :--size 0 f)
              (c/exec :chown cockroach-user f))))))

    db/LogFiles
    (log-files [_ test node] log-files)))

(defn with-idempotent
  "Takes a predicate on operation functions, and an op, presumably resulting
  from a client call. If (idempotent? (:f op)) is truthy, remaps :info types to
  :fail."
  [idempotent? op]
  (if (and (idempotent? (:f op)) (= :info (:type op)))
    (assoc op :type :fail)
    op))

(defmacro with-timeout
  "Like util/timeout, but throws (RuntimeException. \"timeout\") for timeouts.
  Throwing means that when we time out inside a with-conn, the connection state
  gets reset, so we don't accidentally hand off the connection to a later
  invocation with some incomplete transaction."
  [& body]
  `(util/timeout timeout-delay
                 (throw (RuntimeException. "timeout"))
                 ~@body))

(defmacro with-txn-retry-as-fail
  "Takes an op, runs body, catches PSQL 'restart transaction' errors, and
  converts them to :fails"
  [op & body]
  `(try ~@body
       (catch org.postgresql.util.PSQLException e#
         (if (re-find #"ERROR: restart transaction" (.getMessage e#))
           (assoc ~op
                  :type :fail
                  :error (str/replace
                           (.getMessage e#) #"ERROR: restart transaction: " ""))
           (throw e#)))))

(defmacro with-txn-retry
  "Catches PSQL 'restart transaction' errors and retries body a bunch of times,
  with exponential backoffs."
  [& body]
  `(util/with-retry [attempts# 30
                     backoff# 20]
     ~@body
     (catch org.postgresql.util.PSQLException e#
       (if (and (pos? attempts#)
                (re-find #"ERROR: restart transaction"
                         (.getMessage e#)))
         (do (Thread/sleep backoff#)
             (~'retry (dec attempts#)
                      (* backoff# (+ 4 (* 0.5 (- (rand) 0.5))))))
         (throw e#)))))

(defmacro with-txn
  "Wrap a evaluation within a SQL transaction."
  [[c conn] & body]
  `(j/with-db-transaction [~c ~conn {:isolation isolation-level}]
     ~@body))

(defn exception->op
  "Takes an exception and maps it to a partial op, like {:type :info, :error
  ...}. nil if unrecognized."
  [e]
  (let [m (.getMessage e)]
    (condp instance? e
      java.sql.SQLTransactionRollbackException
      {:type :fail, :error [:rollback m]}

      java.sql.BatchUpdateException
      (if (re-find #"getNextExc" m)
        ; Wrap underlying exception error with [:batch ...]
        (when-let [op (exception->op (.getNextException e))]
          (update op :error (partial vector :batch)))
        {:type :info, :error [:batch-update m]})

      org.postgresql.util.PSQLException
      (condp re-find (.getMessage e)
        #"Connection .+? refused"
        {:type :fail, :error :connection-refused}

        #"context deadline exceeded"
        {:type :fail, :error :context-deadline-exceeded}

        #"rejecting command with timestamp in the future"
        {:type :fail, :error :reject-command-future-timestamp}

        #"encountered previous write with future timestamp"
        {:type :fail, :error :previous-write-future-timestamp}

        #"restart transaction"
        {:type :fail, :error [:restart-transaction m]}

        {:type :info, :error [:psql-exception m]})

      clojure.lang.ExceptionInfo
      (condp = (:type (ex-data e))
        :conn-not-ready {:type :fail, :error :conn-not-ready}
        nil)

      (condp re-find m
        #"^timeout$"
        {:type :info, :error :timeout}

        nil))))

(defmacro with-exception->op
  "Takes an operation and a body. Evaluates body, catches exceptions, and maps
  them to ops with :type :info and a descriptive :error."
  [op & body]
  `(try ~@body
        (catch Exception e#
          (if-let [ex-op# (exception->op e#)]
            (merge ~op ex-op#)
            (throw e#)))))

(defn wait-for-conn
  "Spins until a client is ready. Somehow, I think exceptions escape from this."
  [client]
  (util/timeout 60000 (throw (RuntimeException. "Timed out waiting for conn"))
                (while (try
                         (rc/with-conn [c client]
                           (j/query c ["select 1"])
                           false)
                         (catch RuntimeException e
                           true)))))

(defn query
  "Like jdbc query, but includes a default timeout."
  ([conn expr]
   (query conn expr {}))
  ([conn expr opts]
   (j/query conn expr (assoc opts :timeout timeout-delay))))

(defn insert!
  "Like jdbc insert!, but includes a default timeout."
  [conn table values]
  (j/insert! conn table values {:timeout timeout-delay}))

(defn update!
  "Like jdbc update!, but includes a default timeout."
  [conn table values where]
  (j/update! conn table values where {:timeout timeout-delay}))

(defn db-time
  "Retrieve the current time (precise, monotonic) from the database."
  [c]
  (cond (= jdbc-mode :pg-local)
        (->> (query c ["select extract(microseconds from now()) as ts"]
                      {:row-fn :ts})
             (first)
             (str))

        true
        (->> (query c ["select cluster_logical_timestamp()*10000000000::decimal as ts"]
                      {:row-fn :ts})
             (first)
             (.toBigInteger)
             (str))))


;;;;;;;;;;;;;;;;;;;;;;;; Common test definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn str->int [str]
  (let [n (read-string str)]
    (if (number? n) n nil)))

(defn basic-test
  "Sets up the test parameters common to all tests."
  [opts]
  (merge
    tests/noop-test
    {:nodes   (if (= jdbc-mode :cdb-cluster) (:nodes opts) [:localhost])
     :name    (str "cockroachdb-" (:name opts)
                   (if (:linearizable opts) "-lin" "")
                   (if (= jdbc-mode :cdb-cluster)
                     (str ":" (:name (:nemesis opts)))
                     "-fake"))
     :db      (db opts)
     :os      (if (= jdbc-mode :cdb-cluster) ubuntu/os os/noop)
     :client  (:client (:client opts))
     :nemesis (if (= jdbc-mode :cdb-cluster)
                (:client (:nemesis opts))
                nemesis/noop)
     :generator (gen/phases
                  (->> (gen/nemesis (:during (:nemesis opts))
                                    (:during (:client opts)))
                       (gen/time-limit (:time-limit opts)))
                  (gen/log "Nemesis terminating")
                  (gen/nemesis (:final (:nemesis opts)))
                  (gen/log "Waiting for quiescence")
                  (gen/sleep (:recovery-time opts))
                  ; Final client
                  (gen/clients (:final (:client opts))))}
    (dissoc opts :name :nodes :client :nemesis)))
