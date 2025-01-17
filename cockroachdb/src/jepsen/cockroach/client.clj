(ns jepsen.cockroach.client
  "For talking to cockroachdb over the network"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen [util :as util :refer [meh]]
                    [reconnect :as rc]]
            [jepsen.cockroach [auto :as auto :refer [cockroach-user
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

(def timeout-delay "Default timeout for operations in ms" 10000)
(def max-timeout "Longest timeout, in ms" 30000)

(def isolation-level "Default isolation level for txns" :serializable)

;; for secure mode
(def client-cert "certs/node.client.crt")
(def client-key "certs/node.client.pk8")
(def ca-cert "certs/ca.crt")

;; Postgres user and dbname for jdbc-mode = :pg-*
(def pg-user "kena") ; must already exist
(def pg-passwd "kena") ; must already exist
(def pg-dbname "mydb") ; must already exist

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
          :subprotocol  "postgresql"
          :loginTimeout  (/ max-timeout 1000)
          :connectTimeout (/ max-timeout 1000)
          :socketTimeout (/ max-timeout 1000)}
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
               (util/timeout max-timeout
                             (throw (RuntimeException.
                                      (str "Connection to " node " timed out")))
                             (util/retry 0.1
                               (let [spec (db-conn-spec node)
                                     conn (j/get-connection spec)
                                     spec' (j/add-connection spec conn)]
                                 (assert spec')
                                 spec'))))
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
       (catch java.sql.SQLException e#
         (if (re-find #"ERROR: restart transaction"
                      (str (exception->op e#)))
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
     (catch java.sql.SQLException  e#
       (if (and (pos? attempts#)
                (re-find #"ERROR: restart transaction"
                         (str (exception->op e#))))
         (do (Thread/sleep backoff#)
             (~'retry (dec attempts#)
                      (* backoff# (+ 4 (* 0.5 (- (rand) 0.5))))))
         (throw e#)))))

(defmacro with-txn
  "Wrap a evaluation within a SQL transaction."
  [[c conn] & body]
  `(j/with-db-transaction [~c ~conn {:isolation isolation-level}]
     ~@body))

(defmacro with-restart
  "Wrap an evaluation within a CockroachDB retry block."
  [c & body]
  `(util/with-retry [attempts# 10
                     backoff# 20]
     (j/execute! ~c ["savepoint cockroach_restart"])
     ~@body
     (catch java.sql.SQLException  e#
       (if (and (pos? attempts#)
                (re-find #"ERROR: restart transaction"
                         (str (exception->op e#))))
         (do (j/execute! ~c ["rollback to savepoint cockroach_restart"])
             (Thread/sleep backoff#)
             (info "txn-restart" attempts#)
             (~'retry (dec attempts#)
              (* backoff# (+ 4 (* 0.5 (- (rand) 0.5))))))
         (throw e#)))))

(defn exception->op
  "Takes an exception and maps it to a partial op, like {:type :info, :error
  ...}. nil if unrecognized."
  [e]
  (when-let [m (.getMessage e)]
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
                         (with-conn [c client]
                           (j/query c ["select 1"])
                           false)
                         (catch RuntimeException e
                           true)))))

(defn query
  "Like jdbc query, but includes a default timeout in ms."
  ([conn expr]
   (query conn expr {}))
  ([conn [sql & params] opts]
   (let [s (j/prepare-statement (j/db-find-connection conn)
                                sql
                                {:timeout (/ timeout-delay 1000)})]
     (try
       (j/query conn (into [s] params) opts)
       (finally
         (.close s))))))

(defn insert!
  "Like jdbc insert!, but includes a default timeout."
  [conn table values]
  (j/insert! conn table values {:timeout timeout-delay}))

(defn insert-with-rowid!
  "Like insert!, but includes the auto-generated :rowid."
  [conn table record]
  (let [keys (->> record
                  keys
                  (map name)
                  (str/join ", "))
        placeholder (str/join ", " (repeat (count record) "?"))]
    (merge record
           (first (query conn
                         (into [(str "insert into " table " (" keys
                                     ") values (" placeholder
                                     ") returning rowid;")]
                               (vals record)))))))

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

(defn split!
  "Split the given table at the given key."
  [conn table k]
  (query conn [(str "alter table " (name table) " split at values ("
                    (if (number? k)
                      k
                      (str "'" k "'"))
                    ")")]))
