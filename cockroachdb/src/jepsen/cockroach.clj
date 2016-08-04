(ns jepsen.cockroach
  "Tests for CockroachDB"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.jdbc :as j]
            [clj-ssh.ssh :as ssh]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [multiset.core :as multiset]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [knossos.op :as op]
            [jepsen
             [client :as client]
             [core :as jepsen]
             [db :as db]
             [os :as os]
             [tests :as tests]
             [control :as c :refer [|]]
             [model :as model]
             [store :as store]
             [checker :as checker]
             [nemesis :as nemesis]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.os.debian :as debian]
            [jepsen.cockroach.nemesis :as cln]
            [jepsen.cockroach.error :as error]))

(import [java.net URLEncoder])

;; timeout for DB operations during tests
(def timeout-delay 3000) ; milliseconds

;; number of simultaneous clients
(def concurrency-factor 30)

(defn control-addr
  "Address of the Jepsen control node, as seen by the local node. Used to
  filter packet captures."
  []
  ; We drop any sudo binding here to make sure we aren't looking at a subshell
  (let [line (binding [c/*sudo* nil]
               (c/exec :env | :grep "SSH_CLIENT"))
        matches (re-find #"SSH_CLIENT=(.+?)\s" line)]
    (assert matches)
    (matches 1)))

(def tcpdump "/usr/sbin/tcpdump")

;; which database to use during the tests.

;; Possible values:
;; :pg-local  Send the test SQL to a PostgreSQL database.
;; :cdb-local  Send the test SQL to a preconfigured local CockroachDB instance.
;; :cdb-cluster Send the test SQL to the CockroachDB cluster set up by the framework.
(def jdbc-mode :cdb-cluster)

;; Unix username to log into via SSH for :cdb-cluster,
;; or to log into to localhost for :pg-local and :cdb-local
(def username "ubuntu")

(def cockroach-user
  "User to run cockroachdb as"
  "cockroach")

;; Isolation level to use with test transactions.
(def isolation-level :serializable)

;; CockroachDB user and db name for jdbc-mode = :cdb-*
(def db-user "root")
(def db-passwd "dummy")
(def db-port 26257)
(def dbname "jepsen") ; will get created automatically

;; for secure mode
(def client-cert "certs/node.client.crt")
(def client-key "certs/node.client.pk8")
(def ca-cert "certs/ca.crt")

;; Postgres user and dbname for jdbc-mode = :pg-*
(def pg-user "kena") ; must already exist
(def pg-passwd "kena") ; must already exist
(def pg-dbname "mydb") ; must already exist

;;;;;;;;;;;;; Cluster settings ;;;;;;;;;;;;;;

;; whether to start the CockroachDB cluster in insecure mode (SSL disabled)
;; (may be useful to capture the network traffic between client and server)
(def insecure true)

;; Extra command-line arguments to give to `cockroach start`
(def cockroach-start-arguments
  (concat [:start
           ;; ... other arguments here ...
           ]
          (if insecure [:--insecure] [])))

;; Home directory for the CockroachDB setup
(def working-path "/opt/cockroach")

;; Location of various files
(def cockroach (str working-path "/cockroach"))
(def store-path (str working-path "/cockroach-data"))
(def log-path (str working-path "/logs"))
(def pidfile (str working-path "/pid"))

(def errlog (str log-path "/cockroach.stderr"))
(def verlog (str log-path "/version.txt"))
(def pcaplog (str log-path "/trace.pcap"))
(def log-files (if (= jdbc-mode :cdb-cluster) [errlog verlog pcaplog] []))

;;;;;;;;;;;;;;;;;;;; Database set-up and access functions  ;;;;;;;;;;;;;;;;;;;;;;;

;; How to extract db time
(defn db-time
  "Retrieve the current time (precise, monotonic) from the database."
  [c]
  (cond (= jdbc-mode :pg-local)
        (->> (j/query c ["select extract(microseconds from now()) as ts"] :row-fn :ts)
             (first)
             (str))

        true
        (->> (j/query c ["select cluster_logical_timestamp()*10000000000::decimal as ts"] :row-fn :ts)
             (first)
             (.toBigInteger)
             (str))))

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

(defn open-conn
  "Given a Jepsen node, opens a new connection."
  [spec]
  (j/add-connection spec (j/get-connection spec)))

(defn close-conn
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (j/db-find-connection conn)]
    (.close c))
  (dissoc conn :connection))

(defn client
  "Constructs a network client for a node."
  [node]
  (error/client
    (fn open []
      (let [spec (db-conn-spec node)
            conn (j/get-connection spec)
            spec' (j/add-connection spec conn)]
        spec'))
    close-conn))

(defn init-conn
  "Given a Jepsen node, create an atom with a connection object therein."
  [node]
  (->> node db-conn-spec open-conn atom))

(defn wrap-env
  [env cmd]
  ["env" env cmd])

(defn cockroach-start-cmdline
  "Construct the command line to start a CockroachDB node."
  [& extra-args]
  (concat
   [:env
    ;;:COCKROACH_TIME_UNTIL_STORE_DEAD=5s
    :start-stop-daemon
    :--start :--background
    :--make-pidfile :--pidfile pidfile
    :--no-close
    :--chuid cockroach-user
    :--chdir working-path
    :--exec (c/expand-path cockroach)
    :--]
   cockroach-start-arguments
   extra-args
   [:--logtostderr :true :>> errlog (c/lit "2>&1")]))

(defn runcmd
  "The command to run cockroach for a given test"
  [test]
  (wrap-env [(str "COCKROACH_LINEARIZABLE="
                 (if (:linearizable test) "true" "false"))
             (str "COCKROACH_MAX_OFFSET=" "250ms")]
            (cockroach-start-cmdline
              [(str "--join=" (name (jepsen/primary test)))])))

(defmacro csql! [& body]
  "Execute SQL statements using the cockroach sql CLI."
  `(c/cd working-path
         (c/exec
          (concat
           [cockroach :sql]
           (if insecure [:--insecure] nil)
           [:-e ~@body]
           [:>> errlog (c/lit "2>&1")]))))

(defn install!
  "Installs CockroachDB on the given node. Test should include a :tarball url
  the tarball."
  [test node]
  (c/su
    (debian/install [:tcpdump :ntpdate])
    (cu/ensure-user! cockroach-user)
    (cu/install-tarball! node (:tarball test) working-path false)
    (c/exec :mkdir :-p working-path)
    (c/exec :mkdir :-p log-path)
    (c/exec :chown :-R (str cockroach-user ":" cockroach-user) working-path))
  (info node "Cockroach installed"))

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
        (install! test node)

        (info node "Setting date!")
        (c/exec :ntpdate :-b cln/ntpserver)
        (jepsen/synchronize test)

        (when (= node (jepsen/primary test))
          (info node "Starting CockroachDB once to initialize cluster...")
          (c/su (c/exec (cockroach-start-cmdline nil)))

          (Thread/sleep 1000)

          (info node "Stopping 1st CockroachDB before starting cluster...")
          (c/exec cockroach :quit (if insecure [:--insecure] [])))

        (jepsen/synchronize test)

        (info node "Starting packet capture (filtering on" (control-addr)
              ")...")
        (c/trace
        (c/su (c/exec :start-stop-daemon
                      :--start :--background
                      :--exec tcpdump
                      :--
                      :-w pcaplog :host (control-addr) :and :port db-port)))

        (info node "Starting CockroachDB...")
        (c/exec cockroach :version :> verlog (c/lit "2>&1"))
        (c/trace (c/su (c/exec (:runcmd test))))

        (info node "Cochroach started")

        (jepsen/synchronize test)

        (when (= node (jepsen/primary test))
          (info node "Creating database...")
          (csql! (str "create database " dbname)))

      (info node "Setup complete")))


    (teardown! [_ test node]
      (when (= jdbc-mode :cdb-cluster)
;        (info node "Resetting the clocks...")
;        (info node (c/su (c/exec :ntpdate :-b cln/ntpserver)))

        (info node "Stopping cockroachdb...")
        (meh (c/exec :timeout :5s cockroach :quit
                     (if insecure [:--insecure] [])))
        (meh (c/exec :killall -9 :cockroach))

        (info node "Erasing the store...")
        (c/exec :rm :-rf store-path)

        (info node "Stopping tcpdump...")
        (meh (c/su (c/exec :killall -9 :tcpdump)))

        (info node "Clearing the logs...")
        (c/su
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

(defmacro with-annotate-errors
  "Replace complex CockroachDB errors by a simple error message."
  [& body]
  `(let [res# (do ~@body)]
     (if (= (:type res#) :fail)
       (let [e# (:error res#)]
         (cond (nil? e#)
               res#

               (re-find #"restart transaction" e#)
               (assoc res# :retry true)

               true
               res#))
       res#)))

;; FIXME: CockroachDB features a custom txn retry protocol, documented in
;; https://www.cockroachlabs.com/docs/build-a-test-app.html#step-4-execute-transactions-from-a-client
;; However it seems that the JDBC driver and/or clojure sqldb package does
;; not enable one to reuse the transaction after an exception was thrown. So
;; we use a (more overweight) traditional retry loop here instead. This
;; may reduce performance.
(defmacro with-txn-retries
  "Retries body on rollbacks. Uses exponential back-off to avoid conflict storms."
  [conn & body]
  `(loop [retry# 30
          trace# []
          backoff# 20]
     (let [res# (do ~@body)]
       (if (:retry res#)
         (if (> retry# 0)
           (do
             (info "Retrying from " res#)
             ;;(let [spec# (close-conn (deref ~conn))
             ;;      new-conn# (open-conn spec#)]
             ;;  (info "Re-opening connection for retry...")
             ;;  (reset! ~conn new-conn#))
             (Thread/sleep backoff#)
             (let [delay# (* backoff# (+ 4 (* 0.5 (- (rand) 0.5))))]
               (recur (- retry# 1) (conj trace# (:error res#)) delay#)))
           (assoc res# :error [:retry-fail trace#]))
         res#))))

(defmacro with-error-handling
  "Report SQL errors as Jepsen error strings."
  [op & body]
  `(try ~@body
        (catch java.sql.SQLTransactionRollbackException e#
          (let [m# (.getMessage e#)]
            (assoc ~op :type :fail, :error (str "SQLTransactionRollbackException: " m#))))
        (catch java.sql.BatchUpdateException e#
          (let [m# (.getMessage e#)
                mm# (if (re-find #"getNextExc" m#)
                      (str "BatchUpdateException: " m# "\n"
                           (.getMessage (.getNextException e#)))
                      m#)]
            (assoc ~op :type :fail, :error mm#)))
        (catch org.postgresql.util.PSQLException e#
          (let [m# (.getMessage e#)]
            (assoc ~op :type :fail, :error (str "PSQLException: " m#))))))

(defmacro with-reconnect
  "Reconnect if failed due to disconnect."
  [conn retry & body]
  `(let [res# (do ~@body)
         uncertain# (and (:error res#)
                         (or (re-find #"This connection has been closed" (:error res#))
                             (re-find #"An I/O error occurred while sending to the backend" (:error res#))
                             (re-find #"Cannot change transaction isolation level in the middle of a transaction." (:error res#))
                             (re-find #"current transaction is aborted, commands ignored until end of transaction block" (:error res#))
                             ))]
     (if uncertain#
       (do
         (with-error-handling res#
           (let [spec# (close-conn (deref ~conn))
                 new-conn# (open-conn spec#)]
             (info "Disconnected; re-opening connection...")
             (reset! ~conn new-conn#)
             res#))
         (if ~retry
           (do ~@body)
           (assoc res# :type :info, :error (str "uncertain: " (:error res#)))))
       res#)))

(defmacro with-timeout
  "Write an evaluation within a timeout check. Re-open the connection
  if the operation time outs."
  [conn alt & body]
  `(util/timeout timeout-delay
                 (do
                   (let [spec# (close-conn (deref ~conn))
                         new-conn# (open-conn spec#)]
                     (info "Re-opening connection...")
                     (reset! ~conn new-conn#))
                   ~alt)
                 ~@body))

(defmacro with-txn
  "Wrap a evaluation within a SQL transaction with timeout."
  [op [c conn] & body]
  `(with-timeout ~conn
     (assoc ~op :type :info, :error [:timeout :url (:subname (deref ~conn))])
     (with-txn-retries ~conn
       (with-annotate-errors
         (with-reconnect ~conn false
           (with-error-handling ~op
             (j/with-db-transaction [~c (deref ~conn)
                                     :isolation isolation-level]
               ~@body)))))))

(defmacro with-txn-notimeout
  "Wrap a evaluation within a SQL transaction without timeout."
  [op [c conn] & body]
  `(with-txn-retries ~conn
     (with-annotate-errors
       (with-reconnect ~conn true
         (with-error-handling ~op
           (j/with-db-transaction [~c (deref ~conn) :isolation isolation-level]
             ~@body))))))

;;;;;;;;;;;;;;;;;;;;;;;; Common test definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn str->int [str]
  (let [n (read-string str)]
    (if (number? n) n nil)))

(defn basic-test
  "Sets up the test parameters common to all tests."
  [opts]
  (let [t (merge
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
                          ; Quiesce nemesis
                          (gen/log "Waiting for quiescence")
                          (->> (gen/nemesis (:final (:nemesis opts)))
                               (gen/time-limit 15))
                          ; Final client
                          (gen/clients (:final (:client opts))))}
            (dissoc opts :name :nodes :client :nemesis))]
    (assoc t :runcmd (runcmd t))))
