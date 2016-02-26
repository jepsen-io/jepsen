(ns jepsen.cockroach
    "Tests for CockroachDB"
    (:require [clojure.tools.logging :refer :all]
            [clojure.java.jdbc :as j]
            [clj-ssh.ssh :as ssh]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [multiset.core :as multiset]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [knossos.op :as op]
            [jepsen [client :as client]
             [core :as jepsen]
             [db :as db]
             [tests :as tests]
             [control :as c :refer [|]]
             [model :as model]
             [checker :as checker]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.ubuntu :as ubuntu]))

(import [java.net URLEncoder])

;; duration of 1 jepsen test
(def test-duration 30) ; seconds

;; duration between interruptions
(def nemesis-delay 5) ; seconds

;; duration of an interruption
(def nemesis-duration 5) ; seconds

;; timeout for DB operations during tests
(def timeout-delay 1500) ; milliseconds

;; number of tables involved in the monotonic-multitable tests
(def multitable-spread 2)

;; which database to use during the tests.

;; Possible values:
;; :pg-local  Send the test SQL to a PostgreSQL database.
;; :cdb-local  Send the test SQL to a preconfigured local CockroachDB instance.
;; :cdb-cluster Send the test SQL to the CockroachDB cluster set up by the framework.
(def jdbc-mode :cdb-cluster)

;; Unix username to log into via SSH for :cdb-cluster,
;; or to log into to localhost for :pg-local and :cdb-local
(def username "ubuntu")

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
(def insecure false)

;; whether to start the CockroachDB cluster with --linearizable
(def linearizable false)

;; Extra command-line arguments to give to `cockroach start`
(def cockroach-start-arguments
  (concat [:start
           ;; ... other arguments here ...
           ]
          (if insecure [:--insecure] [])
          (if linearizable [:--linearizable] [])))

;; Home directory for the CockroachDB setup
(def working-path "/home/ubuntu")

;; Location of various files
(def cockroach (str working-path "/cockroach"))
(def store-path (str working-path "/cockroach-data"))
(def log-path (str working-path "/logs"))
(def pidfile (str working-path "/pid"))
(def errlog (str log-path "/cockroach.stderr"))
(def verlog (str log-path "/version.txt"))

;; Location of the custom utility compiled from scripts/adjtime.c
(def adjtime (str working-path "/adjtime"))
;; NTP server to use with `ntpdate`
(def ntpserver "ntp.ubuntu.com")

(def log-files (if (= jdbc-mode :cdb-cluster) [errlog verlog] []))

;;;;;;;;;;;;;;;;;;;; Database set-up and access functions  ;;;;;;;;;;;;;;;;;;;;;;;

;; How to extract db time
(def db-timestamp
  (cond (= jdbc-mode :pg-local) "extract(microseconds from now())"
        true "extract(epoch_nanoseconds from now())"))

(def ssl-settings
  (if insecure ""
      (str "?ssl=true"
           "&sslcert=" client-cert
           "&sslkey=" client-key
           "&sslrootcert=" ca-cert
           "&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory"))
  )

(defn db-conn-spec
   "Assemble a JDBC connection specification for a given Jepsen node."
  [node]
  (merge {:classname  "org.postgresql.Driver"  :subprotocol "postgresql"}
         (cond (= jdbc-mode :cdb-cluster)
               {:subname      (str "//" (name node) ":" db-port "/" dbname ssl-settings)
                :user        db-user
                :password    db-passwd}
               (= jdbc-mode :cdb-local)
               {:subname      (str "//localhost:" db-port "/" dbname ssl-settings)
                :user        db-user
                :password    db-passwd}
               (= jdbc-mode :pg-local)
               {:subname      (str "//localhost/" pg-dbname)
                :user        pg-user
                :password    pg-passwd
                })))

(defn open-conn
  "Given a Jepsen node, opens a new connection."
  [node]
  (let [spec (db-conn-spec node)]
    (j/add-connection spec (j/get-connection spec))))

(defn close-conn
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (:connection conn)]
    (.close c))
  (dissoc conn :connection))

(defn cockroach-start-cmdline
  "Construct the command line to start a CockroachDB node."
  [extra-arg]
  (concat
   [:start-stop-daemon
    :--start :--background
    :--make-pidfile :--pidfile pidfile
    :--no-close
    :--chuid username
    :--chdir working-path
    :--exec (c/expand-path cockroach)
    :--]
   cockroach-start-arguments
   extra-arg
   [:--logtostderr :true :>> errlog (c/lit "2>&1")]))

(defmacro csql! [& body]
  "Execute SQL statements using the cockroach sql CLI."
  `(c/trace
    (c/cd working-path
          (apply c/exec
                 (concat
                  [cockroach :sql]
                  (if insecure [:--insecure] nil)
                  [:-e ~@body]
                  [:>> errlog (c/lit "2>&1")]))
          )))


(defn db
  "Sets up and tears down CockroachDB."
  []
  (reify db/DB
    (setup! [_ test node]

      (when (= jdbc-mode :cdb-cluster)
        (when (= node (jepsen/primary test))
          (info node "Starting CockroachDB once to initialize cluster...")
          (c/trace (c/su (apply c/exec (cockroach-start-cmdline nil))))
          (Thread/sleep 1000)

          (info node "Stopping 1st CockroachDB before starting cluster...")
          (c/exec :killall -9 :cockroach))

        (jepsen/synchronize test)

        (info node "Starting CockroachDB...")
        (c/exec cockroach :version :> verlog (c/lit "2>&1"))
        (c/trace (c/su (apply c/exec
                              (cockroach-start-cmdline
                               [(str "--join=" (name (jepsen/primary test)))]))))


        (jepsen/synchronize test)

        (when (= node (jepsen/primary test))
          (info node "Creating database...")
          (csql! (str "create database " dbname)))

        (jepsen/synchronize test)
      )

      (info node "Testing JDBC connection...")
      (let [conn (open-conn node)]
        (j/with-db-transaction [c conn :isolation isolation-level]
          (into [] (j/query c ["select 1"])))
        (close-conn conn))

      (info node "Setup complete")
      (Thread/sleep 1000))


    (teardown! [_ test node]

      (when (= jdbc-mode :cdb-cluster)
        (info node "Stopping cockroachdb...")
        (meh (c/exec :killall -9 :cockroach))

        (info node "Resetting the clocks...")
        (c/su (c/exec :ntpdate ntpserver))

        (info node "Erasing the store...")
        (c/exec :rm :-rf store-path)

        (info node "Clearing the logs...")
        (apply c/exec :truncate :-c :--size 0 log-files)

        (jepsen/synchronize test)
        ))


    db/LogFiles
    (log-files [_ test node] log-files)
    )
  )

(defmacro with-error-handling
  "Report SQL errors as Jepsen error strings."
  [op & body]
  `(try ~@body
        (catch java.sql.SQLTransactionRollbackException e#
          (let [m# (.getMessage e#)]
            (assoc ~op :type :fail, :error m#)))
        (catch java.sql.BatchUpdateException e#
          (let [m# (.getMessage e#)
                mm# (if (re-find #"getNextExc" m#)
                      (str m# "\n" (.getMessage (.getNextException e#)))
                      m#)]
            (assoc ~op :type :fail, :error mm#)))
        (catch org.postgresql.util.PSQLException e#
          (let [m# (.getMessage e#)]
            (assoc ~op :type :fail, :error m#)))
        ))

(defmacro capture-txn-abort
  "Converts aborted transactions to an ::abort keyword"
  [& body]
  `(try ~@body
        ; Galera
        (catch java.sql.SQLTransactionRollbackException e#
          (if (re-find #"abort" (.getMessage e#))
            ::abort
            (throw e#)))
        (catch java.sql.BatchUpdateException e#
          (if (re-find #"abort" (.getMessage e#))
            ::abort
            (throw e#)))))

(defmacro with-txn-aborts
  "Aborts body on rollbacks."
  [op & body]
  `(let [res# (capture-txn-abort ~@body)]
     (if (= ::abort res#)
       (assoc ~op :type :fail, :error :aborted)
       res#)))

(defmacro with-txn
  "Wrap a evaluation within a SQL transaction with timeout."
  [op [c conn] & body]
  `(util/timeout timeout-delay (assoc ~op :type :info, :value :timeout)
                 (with-error-handling ~op
                   ;(with-txn-aborts ~op
                     (j/with-db-transaction [~c ~conn :isolation isolation-level]
                       ~@body))))

(defn db-time
  "Retrieve the current time (precise) from the database."
  [c]
  (->> (j/query c [(str "select " db-timestamp " as ts")])
       (mapv :ts)
       (first)))


;;;;;;;;;;;;;;;;;;;;;;;; Common test definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn str->int [str]
  (let [n (read-string str)]
    (if (number? n) n nil)))

(defn clock-milli-scrambler
  "Randomizes the system clock of all nodes within a dt-millisecond window."
  [dt]
  (reify client/Client
    (setup! [this test _]
      this)

    (invoke! [this test op]
      (assoc op :value
             (c/on-many (:nodes test)
                        (c/su
                         (c/exec adjtime (str (- (rand-int (* 2 dt)) dt)))))))

    (teardown! [this test]
      (c/on-many (:nodes test)
                 (c/su (c/exec :ntpdate ntpserver))))
    ))

(defn basic-test
  "Sets up the test parameters common to all tests."
  [nodes opts]
  (merge tests/noop-test
         (let [t {:nodes   (if (= jdbc-mode :cdb-cluster) nodes [:localhost])
                  :name    (str "cockroachdb-" (:name opts))
                  :db      (db)
                  :ssh     {:username username :strict-host-key-checking false}
                  }]
           (if (= jdbc-mode :cdb-cluster)
             (assoc t :nemesis (nemesis/partition-random-halves)
                    :os      ubuntu/os)
             t))
         (dissoc opts :name)))

                                        ;
;;;;;;;;;;;;;;;;;; Atomic counter test ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn atomic-client
  "A client for a single compare-and-set register."
  [conn]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (open-conn node)]
        (when (= node (jepsen/primary test))
          (j/execute! conn ["drop table if exists test"])
          (j/execute! conn ["create table test (name varchar, val int)"])
          (j/insert! conn :test {:name "a" :val 0}))
        (atomic-client (open-conn node))))

    (invoke! [this test op]
      (with-txn op [c conn]
        (case (:f op)
          :read (->> (j/query c ["select val from test"])
                     (mapv :val)
                     (first)
                     (assoc op :type :ok, :value))

          :write (do
                   (j/update! c :test {:val (:value op)} ["name = 'a'"])
                   (assoc op :type :ok))

          :cas (let [[value' value] (:value op)
                     cnt (j/update! c :test {:val value} ["name='a' and val = ?" value'])]
                 (assoc op :type (if (zero? (first cnt)) :fail :ok))))
        ))

    (teardown! [_ test]
      (util/timeout timeout-delay nil
                    (j/execute! conn ["drop table if exists test"]))
      (close-conn conn))
    ))

(defn atomic-test
  [nodes]
  (basic-test nodes
   {
    :name    "atomic"
    :client  (atomic-client nil)
    :generator (gen/phases
                (->> (gen/mix [r w cas])
                    (gen/stagger 1)
                    (gen/nemesis
                     (gen/seq (cycle [(gen/sleep nemesis-delay)
                                      {:type :info, :f :start}
                                      (gen/sleep nemesis-duration)
                                      {:type :info, :f :stop}])))
                    (gen/time-limit test-duration))
                (gen/nemesis (gen/once {:type :info, :f :stop}))
                (gen/sleep 5))

    :model   (model/cas-register 0)
    :checker (checker/compose
              {:perf   (checker/perf)
               :details checker/linearizable})
    }
   ))

;;;;;;;;;;;;;;;;;;;;;;;;;; Distributed set test ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn check-sets
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted, and that all
  elements are unique."
  []
  (reify checker/Checker
    (check [this test model history]
      (let [attempts (->> history
                          (r/filter op/invoke?)
                          (r/filter #(= :add (:f %)))
                          (r/map :value)
                          (into #{}))
            adds (->> history
                      (r/filter op/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            final-read-l (->> history
                          (r/filter op/ok?)
                          (r/filter #(= :read (:f %)))
                          (r/map :value)
                          (reduce (fn [_ x] x) nil))]
        (if-not final-read-l
          {:valid? false
           :error  "Set was never read"})

        (let [final-read  (into #{} final-read-l)

              dups        (into [] (for [[id freq] (frequencies final-read-l) :when (> freq 1)] id))

              ;;The OK set is every read value which we tried to add
              ok          (set/intersection final-read attempts)

              ;; Unexpected records are those we *never* attempted.
              unexpected  (set/difference final-read attempts)

              ;; Lost records are those we definitely added but weren't read
              lost        (set/difference adds final-read)

              ;; Recovered records are those where we didn't know if the add
              ;; succeeded or not, but we found them in the final set.
              recovered   (set/difference ok adds)]

          {:valid?          (and (empty? lost) (empty? unexpected) (empty? dups))
           :duplicates      dups
           :ok              (util/integer-interval-set-str ok)
           :lost            (util/integer-interval-set-str lost)
           :unexpected      (util/integer-interval-set-str unexpected)
           :recovered       (util/integer-interval-set-str recovered)
           :ok-frac         (util/fraction (count ok) (count attempts))
           :unexpected-frac (util/fraction (count unexpected) (count attempts))
           :lost-frac       (util/fraction (count lost) (count attempts))
           :recovered-frac  (util/fraction (count recovered) (count attempts))})))))

(defn sets-client
  [conn]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (open-conn node)]
        (when (= node (jepsen/primary test))
          (j/execute! conn ["drop table if exists set"])
          (j/execute! conn ["create table set (val int)"]))

        (sets-client (open-conn node))))

    (invoke! [this test op]
      (with-txn op [c conn]
        (case (:f op)
          :add  (do
                  (j/insert! c :set {:val (:value op)})
                  (assoc op :type :ok))
          :read (->> (j/query c ["select val from set"])
                     (mapv :val)
                     (assoc op :type :ok, :value))
          )))

    (teardown! [_ test]
      (util/timeout timeout-delay nil
                     (j/execute! conn ["drop table if exists set"]))
       (close-conn conn))
      ))

(defn with-nemesis
  "Wraps a client generator in a nemesis that induces failures and eventually
  stops."
  [client]
  (gen/phases
   (->> client
        (gen/nemesis
         (gen/seq (cycle [(gen/sleep nemesis-delay)
                          {:type :info, :f :start}
                          (gen/sleep nemesis-duration)
                          {:type :info, :f :stop}
                          ])))
        (gen/time-limit test-duration))
   (gen/nemesis (gen/once {:type :info, :f :stop}))
   (gen/sleep 5)))


(defn sets-test
  [nodes]
  (basic-test nodes
   {
    :name    "set"
    :client (sets-client nil)
    :generator (gen/phases
                (->> (range)
                     (map (partial array-map
                                   :type :invoke
                                   :f :add
                                   :value))
                     gen/seq
                     (gen/stagger 1/10)
                     with-nemesis)
                (->> {:type :invoke, :f :read, :value nil}
                     gen/once
                     gen/clients))
    :checker (checker/compose
              {:perf (checker/perf)
               :details  (check-sets)})
    }
   ))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; Montonic inserts with dependency ;;;;;;;;;;;;;;;;;;;;

(defn check-order
  "Detect monotonicity errors over a result set (internal function)."
  [prev rows res1 res2 ]
  (if (empty? rows) [res1 res2]
      (let [row (first rows)
            nres1 (concat res1 (if (> (nth prev 1) (nth row 1)) (list (list prev row)) ()))
            nres2 (concat res2 (if (>= (first prev) (first row)) (list (list prev row)) ()))]
        (check-order row (rest rows) nres1 nres2))))

(defn monotonic-order
  "Detect monotonicity errors over a result set."
  [rows]
  (check-order (first rows) (rest rows) () ()))

(defn check-monotonic
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was succesful, and that all
  elements are unique and in the same order as database timestamps."
  []
  (reify checker/Checker
    (check [this test model history]
      (let [all-adds-l (->> history
                            (r/filter op/ok?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (into []))
            final-read-l (->> history
                              (r/filter op/ok?)
                              (r/filter #(= :read (:f %)))
                              (r/map :value)
                              (reduce (fn [_ x] x) nil)
                              )
            all-fails (->> history
                           (r/filter op/fail?))]
        (if-not final-read-l
          {:valid? false
           :error  "Set was never read"})
        (let [
              retries    (->> all-fails
                              (r/filter #(= :retry (:error %)))
                              (into []))
              aborts    (->> all-fails
                             (r/filter #(= :abort-uncertain (:error %)))
                             (into []))
              [off-order-sts off-order-val] (monotonic-order final-read-l)

              dups        (into [] (for [[id freq] (frequencies (map first final-read-l)) :when (> freq 1)] id))

              ;; Lost records are those we definitely added but weren't read
              lost        (set/difference (into #{} (map first all-adds-l)) (into #{} (map first final-read-l)))]
          {:valid?          (and (empty? lost) (empty? dups) (empty? off-order-sts) (empty? off-order-val))
           :retry-frac      (util/fraction (count retries) (count history))
           :abort-frac      (util/fraction (count aborts) (count history))
           :lost            (util/integer-interval-set-str lost)
           :lost-frac       (util/fraction (count lost) (count all-adds-l))
           :duplicates      dups
           :order-by-errors off-order-sts
           :value-reorders  off-order-val
           }
          )))))


(defn monotonic-client
  [conn nodenum]
  (reify client/Client
    (setup! [_ test node]
      (let [n (if (= jdbc-mode :cdb-cluster) (str->int (subs (name node) 1 2)) 1)
            conn (open-conn node)]
        (info "Setting up client " n " for " (name node))

        (when (= node (jepsen/primary test))
          (j/execute! conn ["drop table if exists mono"])
          (j/execute! conn ["create table mono (val int, sts bigint, node int, tb int)"])
          (j/insert! conn :mono {:val -1 :sts 0 :node -1 :tb -1}))

        (monotonic-client (open-conn node) n)))

    (invoke! [this test op]
      (with-txn op [c conn]
        (case (:f op)
          :add  (let [curmax (->> (j/query c ["select max(val) as m from mono"] :row-fn :m)
                                  (first))
                      currow (->> (j/query c ["select * from mono where val = ?" curmax])
                                  (map (fn [row] (list (:val row) (:sts row) (:node row) (:tb row))))
                                  (first))
                      dbtime (db-time c)]
                  (j/insert! c :mono {:val (+ 1 curmax) :sts dbtime :node nodenum :tb 0})
                  (assoc op :type :ok, :value currow))

          :read (->> (j/query c ["select * from mono order by sts"])
                     (map (fn [row] (list (:val row) (:sts row) (:node row) (:tb row))))
                     (into [])
                     (assoc op :type :ok, :value))
          )))
    
    (teardown! [_ test]
      (util/timeout timeout-delay nil
                    (j/execute! conn ["drop table if exists mono"]))
      (close-conn conn))
    ))


(defn monotonic-test
  [nodes]
  (basic-test nodes
   {
    :name    "monotonic-parts"
    :client (monotonic-client nil nil)
    :generator (gen/phases
                (->> (range)
                     (map (partial array-map
                                   :type :invoke
                                   :f :add
                                   :value))
                     gen/seq
                     (gen/stagger 1/10)
                     with-nemesis)
                (->> {:type :invoke, :f :read, :value nil}
                     gen/once
                     gen/clients))
    :checker (checker/compose
              {:perf (checker/perf)
               :details (check-monotonic)})
    }
   ))

(defn monotonic-test-skews
  [nodes]
  (let [t (basic-test nodes
   {
    :name    "monotonic-skews"
    :client (monotonic-client nil nil)
    :generator (gen/phases
                (->> (range)
                     (map (partial array-map
                                   :type :invoke
                                   :f :add
                                   :value))
                     gen/seq
                     (gen/stagger 1/10)
                     with-nemesis)
                (->> {:type :invoke, :f :read, :value nil}
                     gen/once
                     gen/clients))
    :checker (checker/compose
              {:perf (checker/perf)
               :details  (check-monotonic)})
    }
   )]
    (if (= jdbc-mode :cdb-cluster)
      (assoc t :nemesis (clock-milli-scrambler 100))
      t)))

;;;;;;;;;;;;;;;;;;;;;; Monotonic inserts over multiple tables ;;;;;;;;;;;;;;;;

(defn check-monotonic-split
  "Same as check-monotonic, but check ordering only from each client's perspective."
  []
  (reify checker/Checker
    (check [this test model history]
      (let [all-adds-l (->> history
                            (r/filter op/ok?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (into []))
            final-read-l (->> history
                              (r/filter op/ok?)
                              (r/filter #(= :read (:f %)))
                              (r/map :value)
                              (reduce (fn [_ x] x) nil)
                              )
            all-fails (->> history
                           (r/filter op/fail?))]
            (if-not final-read-l
              {:valid? false
               :error  "Set was never read"})

            (let [final-read  (into #{} final-read-l)
                  all-adds    (into #{} all-adds-l)

                  retries    (->> all-fails
                                  (r/filter #(= :retry (:error %)))
                                  (into []))
                  aborts    (->> all-fails
                                 (r/filter #(= :abort-uncertain (:error %)))
                                 (into []))
                  sub-lists (->> (range 5)
                                 (map (fn [x] (->> final-read-l
                                                   (r/filter #(= x (nth % 2)))
                                                   (into ())
                                                   (reverse))))
                                 (into []))
                  off-order-pairs (map monotonic-order sub-lists)

                  off-order-sts (map first off-order-pairs)
                  off-order-val (map last off-order-pairs)

                  dups        (into [] (for [[id freq] (frequencies (into [] (map first final-read-l))) :when (> freq 1)] id))

                  ; Lost records are those we definitely added but weren't read
                  lost        (into [] (set/difference
                                        (into #{} all-adds)
                                        (into #{} (map first final-read))))]
              {:valid?          (and (empty? lost)
                                     (empty? dups)
                                     (and (into [] (map empty? off-order-sts)))
                                     (and (into [] (map empty? off-order-val))))
               :retry-frac      (util/fraction (count retries) (count history))
               :abort-frac      (util/fraction (count aborts) (count history))
               :lost            (into [] lost)
               :lost-frac       (util/fraction (count lost) (count all-adds-l))
               :duplicates      dups
               :order-by-errors off-order-sts
               :value-reorders  off-order-val
               })))))

(defn monotonic-multitable-client
  [conn nodenum]
  (reify client/Client
    (setup! [_ test node]
      (let [n (if (= jdbc-mode :cdb-local) (str->int (subs (name node) 1 2)) 1)
            conn (open-conn node)]

        (info "Setting up client " n " for " (name node))

        (when (= node (jepsen/primary test))
          (dorun (for [x (range multitable-spread)]
                   (do
                     (j/execute! conn [(str "drop table if exists mono" x)])
                     (j/execute! conn [(str "create table mono" x
                                            " (val int, sts bigint, node int, tb int)")])
                     (j/insert! conn (str "mono" x) {:val -1 :sts 0 :node -1 :tb x})
                     ))))

        (monotonic-multitable-client (open-conn node) n)))


    (invoke! [this test op]
      (with-txn op [c conn]
        (case (:f op)
          :add  (let [rt (rand-int multitable-spread)
                      dbtime (db-time c)]
                  (j/insert! c (str "mono" rt) {:val (:value op) :sts dbtime :node nodenum :tb rt})
                  (assoc op :type :ok))

          :read (->> (range multitable-spread)
                     (map (fn [x]
                            (->> (j/query c [(str "select * from mono" x " where node <> -1")])
                                 (map (fn [row] (list (:val row) (:sts row) (:node row) (:tb row))))
                                 )))
                     (reduce concat)
                     (sort-by (fn [x] (nth x 1)))
                     (into [])
                     (assoc op :type :ok, :value))
        )))

    (teardown! [_ test]
      (dorun (for [x (range multitable-spread)]
               (util/timeout timeout-delay nil
                             (j/execute! conn [(str "drop table if exists mono" x)]))))
      (close-conn conn))
))

(defn monotonic-multitable-test
  [nodes]
  (basic-test nodes
   {
    :name    "monotonic-spread-parts"
    :client (monotonic-multitable-client nil nil)
    :generator (gen/phases
                (->> (range)
                     (map (partial array-map
                                   :type :invoke
                                   :f :add
                                   :value))
                     gen/seq
                     (gen/stagger 1/10)
                     with-nemesis)
                (->> {:type :invoke, :f :read, :value nil}
                     gen/once
                     gen/clients)
                (gen/sleep 3))
    :checker (checker/compose
              {:perf (checker/perf)
               :details  (check-monotonic-split)})
    }
   ))

(defn monotonic-multitable-test-skews
  [nodes]
  (let [t (basic-test nodes
   {
    :name    "monotonic-spread-skews"
    :client (monotonic-multitable-client nil nil)
    :generator (gen/phases
                (->> (range)
                     (map (partial array-map
                                   :type :invoke
                                   :f :add
                                   :value))
                     gen/seq
                     (gen/stagger 1/10)
                     with-nemesis)
                (->> {:type :invoke, :f :read, :value nil}
                     gen/once
                     gen/clients))
    :checker (checker/compose
              {:perf (checker/perf)
               :details  (check-monotonic-split)})
    }
   )]
    (if (= jdbc-mode :cdb-cluster)
      (assoc t :nemesis (clock-milli-scrambler 100))
      t)))


;;;;;;;;;;;;;;;;;;;;;;;;;;; Test for transfers between bank accounts ;;;;;;;;;;;;;;;;;;;;;;;

(defrecord BankClient [conn n starting-balance]
  client/Client
    (setup! [this test node]
      (let [conn (open-conn node)]

        (when (= node (jepsen/primary test))
          ;; Create initial accounts.
          (j/execute! conn ["drop table if exists accounts"])
          (j/execute! conn ["create table accounts (id int not null primary key, balance bigint not null)"])
          (dotimes [i n]
            (j/insert! conn :accounts {:id i :balance starting-balance})))

        (assoc this :conn conn))
      )

    (invoke! [this test op]
      (with-txn op [c conn]
        (case (:f op)
          :read (->> (j/query c ["select balance from accounts"])
                     (mapv :balance)
                     (assoc op :type :ok, :value))

          :transfer (let [{:keys [from to amount]} (:value op)
                          b1 (-> c
                                 (j/query ["select balance from accounts where id = ?" from]
                                          :row-fn :balance)
                                 first
                                 (- amount))
                          b2 (-> c
                                 (j/query ["select balance from accounts where id = ?" to]
                                          :row-fn :balance)
                                 first
                                 (+ amount))]
                      (cond (neg? b1)
                            (assoc op :type :fail, :value [:negative from b1])

                            (neg? b2)
                            (assoc op :type :fail, :value [:negative to b2])

                            true
                            (do (j/update! c :accounts {:balance b1} ["id = ?" from])
                                (j/update! c :accounts {:balance b2} ["id = ?" to])
                                (assoc op :type :ok))))
                 )))

    (teardown! [_ test]
      (util/timeout timeout-delay nil
                    (j/execute! conn ["drop table if exists accounts"]))
      (close-conn conn))
    )

(defn bank-client
  "Simulates bank account transfers between n accounts, each starting with
  starting-balance."
  [n starting-balance]
  (BankClient. nil n starting-balance))

(defn bank-read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [test process]
  (let [n (-> test :client :n)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-int n)
             :to     (rand-int n)
             :amount (rand-int 5)}}))

(def bank-diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              bank-transfer))

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test model history]
      (let [bad-reads (->> history
                           (r/filter op/ok?)
                           (r/filter #(= :read (:f %)))
                           (r/map (fn [op]
                                  (let [balances (:value op)]
                                    (cond (not= (:n model) (count balances))
                                          {:type :wrong-n
                                           :expected (:n model)
                                           :found    (count balances)
                                           :op       op}

                                         (not= (:total model)
                                               (reduce + balances))
                                         {:type :wrong-total
                                          :expected (:total model)
                                          :found    (reduce + balances)
                                          :op       op}))))
                           (r/filter identity)
                           (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defn bank-test
  [nodes n initial-balance]
  (basic-test nodes
    {:name "bank"
     ;:concurrency 20
     :model  {:n n :total (* n initial-balance)}
     :client (bank-client n initial-balance)
     :generator (gen/phases
                  (->> (gen/mix [bank-read bank-diff-transfer])
                       (gen/clients)
                       (gen/stagger 1/10)
                       (gen/nemesis
                        (gen/seq (cycle [(gen/sleep nemesis-delay)
                                         {:type :info, :f :start}
                                         (gen/sleep nemesis-duration)
                                         {:type :info, :f :stop}])))
                       (gen/time-limit test-duration))
                  (gen/nemesis (gen/once {:type :info, :f :stop}))
                  (gen/log "waiting for quiescence")
                  (gen/sleep 5)
                  (gen/clients (gen/once bank-read)))
     :checker (checker/compose
                {:perf (checker/perf)
                 :details (bank-checker)})}))

(defn bank-test-skews
  [nodes n initial-balance]
  (let [t (basic-test nodes
                      {:name "bank-skews"
                       :model  {:n n :total (* n initial-balance)}
                       :client (bank-client n initial-balance)
                       :generator (gen/phases
                                   (->> (gen/mix [bank-read bank-diff-transfer])
                                        (gen/clients)
                                        (gen/stagger 1/10)
                                        (gen/nemesis
                                         (gen/seq (cycle [(gen/sleep nemesis-delay)
                                                          {:type :info, :f :start}
                                                          (gen/sleep nemesis-duration)
                                                          {:type :info, :f :stop}])))
                                        (gen/time-limit test-duration))
                                   (gen/nemesis (gen/once {:type :info, :f :stop}))
                                   (gen/log "waiting for quiescence")
                                   (gen/sleep 5)
                                   (gen/clients (gen/once bank-read)))
                       :checker (checker/compose
                                 {:perf (checker/perf)
                                  :details (bank-checker)})})]
    (if (= jdbc-mode :cdb-cluster)
      (assoc t :nemesis (clock-milli-scrambler 100))
      t)))

