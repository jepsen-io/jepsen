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
            [jepsen [client :as client]
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
            [jepsen.cockroach-nemesis :as cln]))

(import [java.net URLEncoder])

;; timeout for DB operations during tests
(def timeout-delay 1500) ; milliseconds

;; number of tables involved in the monotonic-multitable tests
(def multitable-spread 2)


;; number of simultaneous clients
(def concurrency-factor 20)

;; address of the Jepsen controlling node as seen by
;; the database nodes. Used to filter packet captures.
;; replace by a string constant e.g. (def control-addr "x.y.z.w")
;; if the following doesnt work.
(def control-addr (.getHostAddress (java.net.InetAddress/getLocalHost)))
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
           :--time-until-store-dead "2s"
           ;; ... other arguments here ...
           ]
          (if insecure [:--insecure] [])))

;; Home directory for the CockroachDB setup
(def working-path "/home/ubuntu")

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
(def db-timestamp
  (cond (= jdbc-mode :pg-local) "extract(microseconds from now())"
        true "extract(epoch_nanoseconds from transaction_timestamp())"))

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
  [spec]
  (j/add-connection spec (j/get-connection spec)))

(defn close-conn
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (:connection conn)]
    (.close c))
  (dissoc conn :connection))

(defn init-conn
  "Given a Jepsen node, create an atom with a connection object therein."
  [node]
  (->> node db-conn-spec open-conn atom))

(defn cockroach-start-cmdline
  "Construct the command line to start a CockroachDB node."
  [& extra-args]
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
   extra-args
   [:--logtostderr :true :>> errlog (c/lit "2>&1")]))

(defmacro csql! [& body]
  "Execute SQL statements using the cockroach sql CLI."
  `(c/cd working-path
         (c/exec
          (concat
           [cockroach :sql]
           (if insecure [:--insecure] nil)
           [:-e ~@body]
           [:>> errlog (c/lit "2>&1")]))
         ))


(defn db
  "Sets up and tears down CockroachDB."
  [linearizable]
  (reify db/DB
    (setup! [_ test node]
      (when (= node (jepsen/primary test))
        (store/with-out-file test "jepsen-version.txt"
          (meh (->> (sh "git" "describe" "--tags")
                    (:out)
                    (print)))))
        
      (when (= jdbc-mode :cdb-cluster)
        (when (= node (jepsen/primary test))
          (info node "Starting CockroachDB once to initialize cluster...")
          (c/su (c/exec (cockroach-start-cmdline nil)))

          (Thread/sleep 1000)
          
          (info node "Stopping 1st CockroachDB before starting cluster...")
          (c/exec cockroach :quit (if insecure [:--insecure] [])))

        (jepsen/synchronize test)

        (info node "Starting packet capture (filtering on " control-addr ")...")
        (c/su (c/exec :start-stop-daemon
                      :--start :--background
                      :--exec tcpdump
                      :--
                      :-w pcaplog :host control-addr :and :port db-port                      
                      ))
        
        (info node "Starting CockroachDB...")
        (c/exec cockroach :version :> verlog (c/lit "2>&1"))
        (c/trace (c/su (c/exec
                        (cockroach-start-cmdline
                         (if linearizable [:--linearizable] [])
                         [(str "--join=" (name (jepsen/primary test)))]))))

        (jepsen/synchronize test)

        (when (= node (jepsen/primary test))
          (info node "Creating database...")
          (csql! (str "create database " dbname)))

        (jepsen/synchronize test)
      )

      (info node "Testing JDBC connection...")
      (let [conn (init-conn node)]
        (j/with-db-transaction [c @conn :isolation isolation-level]
          (into [] (j/query c ["select 1"])))
        (close-conn @conn))

      (info node "Setup complete")
      )


    (teardown! [_ test node]

      (when (= jdbc-mode :cdb-cluster)
        (info node "Resetting the clocks...")
        (c/su (c/exec :ntpdate cln/ntpserver))

        (info node "Stopping cockroachdb...")
        (meh (c/exec cockroach :quit (if insecure [:--insecure] [])))
        (meh (c/exec :killall -9 :cockroach))

        (info node "Erasing the store...")
        (c/exec :rm :-rf store-path)

        (info node "Stopping tcpdump...")
        (meh (c/su (c/exec :killall -9 :tcpdump)))

        (info node "Clearing the logs...")
        (meh (c/su (c/exec :chown username log-files)))
        (c/exec :truncate :-c :--size 0 log-files)

        (jepsen/synchronize test)
        ))


    db/LogFiles
    (log-files [_ test node] log-files)
    )
  )

(defmacro with-annotate-errors
  "Replace complex CockroachDB errors by a simple error message."
  [& body]
  `(let [res# (do ~@body)]
     (if (= (:type res#) :fail)
       (let [e# (:error res#)]
         (cond (nil? e#)
               res#

               (re-find #"read.*encountered previous write.*uncertainty interval" e#)
               (assoc res# :error :retry-uncertainty)
               
               (re-find #"retry txn" e#)
               (assoc res# :error :retry-read-write-conflict)
               
               (re-find #"txn.*failed to push" e#)
               (assoc res# :error :retry-read-write-conflict)
               
               (re-find #"txn aborted" e#)
               (assoc res# :error :retry-write-write-conflict)
               
               true
               res#))
       res#)))

(defmacro with-txn-retries
  "Retries body on rollbacks. Uses exponential back-off to avoid conflict storms."
  [conn & body]
  `(loop [retry# 10
          trace# []
          backoff# 10]
     (let [res# (do ~@body)]
       (if (some #(= (:error res#) %) [:retry-uncertainty
                                       :retry-read-write-conflict
                                       :retry-read-write-conflict
                                       :retry-write-write-conflict])
         (if (> retry# 0)
           (do
             ;;(let [spec# (close-conn (deref ~conn))
             ;;      new-conn# (open-conn spec#)]
             ;;  (info "Re-opening connection for retry...")
             ;;  (reset! ~conn new-conn#))
             (Thread/sleep backoff#)
             (recur (- retry# 1) (conj trace# (:error res#)) (* backoff# 2)))
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
            (assoc ~op :type :fail, :error (str "PSQLException: " m#)))
        )))

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
     (assoc ~op :type :info, :value [:timeout :url (:subname (deref ~conn))])
     (with-txn-retries ~conn
       (with-annotate-errors
         (with-error-handling ~op
           (j/with-db-transaction [~c (deref ~conn) :isolation isolation-level]
             ~@body))))))

(defmacro with-txn-notimeout
  "Wrap a evaluation within a SQL transaction without timeout."
  [op [c conn] & body]
  `(with-txn-retries ~conn
     (with-annotate-errors
       (with-error-handling ~op
         (j/with-db-transaction [~c (deref ~conn) :isolation isolation-level]
           ~@body)))))
  
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

(defn basic-test
  "Sets up the test parameters common to all tests."
  [nodes nemesis linearizable opts]
  (merge tests/noop-test
         {:nodes   (if (= jdbc-mode :cdb-cluster) nodes [:localhost])
          :name    (str "cockroachdb-" (:name opts)
                        (if linearizable "-lin" "")
                        (if (= jdbc-mode :cdb-cluster)
                          (str ":" (:name nemesis))
                          "-fake"))
          :db      (db linearizable)
          :ssh     {:username username :strict-host-key-checking false}
          :os      (if (= jdbc-mode :cdb-cluster) ubuntu/os os/noop)
          :nemesis (if (= jdbc-mode :cdb-cluster) (:client nemesis) nemesis/noop)
          :linearizable linearizable
          }
         (dissoc opts :name)
         ))



;;;;;;;;;;;;;;;;;; Atomic counter test ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord AtomicClient [tbl-created?]
  client/Client

  (setup! [this test node]
    (let [conn (init-conn node)]
      (info node "Connected")
      ;; Everyone's gotta block until we've made the table.
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (info node "Creating table")
          (j/execute! @conn ["drop table if exists test"])
          (j/execute! @conn ["create table test (id int, val int)"])))
      
      (assoc this :conn conn)))
  
  (invoke! [this test op]
    (let [conn (:conn this)]
      (with-txn op [c conn]
        (let [id     (key (:value op))
              value  (val (:value op))
              val'    (->> (j/query c ["select val from test where id = ?" id] :row-fn :val)
                           (first))]
          (case (:f op)
            :read (assoc op :type :ok, :value (independent/tuple id val'))
            
            :write (do
                     (if (nil? val')
                       (j/insert! c :test {:id id :val value})
                       (j/update! c :test {:val value} ["id = ?" id]))
                     (assoc op :type :ok))
            
            :cas (let [[value' value] value
                       cnt (j/update! c :test {:val value} ["id = ? and val = ?" id value'])]
                   (assoc op :type (if (zero? (first cnt)) :fail :ok))))
          ))))
  
  (teardown! [this test]
    (let [conn (:conn this)]
      (meh (with-timeout conn nil
             (j/execute! @conn ["drop table test"])))
      (close-conn @conn))
    ))


(defn atomic-test
  [nodes nemesis linearizable]
  (basic-test nodes nemesis linearizable
              {:name    "atomic"
               :concurrency concurrency-factor
               :client  (AtomicClient. (atom false))
               :generator (->> (independent/sequential-generator
                                (range)
                                (fn [k]
                                  (->> (gen/reserve 5 (gen/mix [w cas]) r)
                                       (gen/delay 1)
                                       (gen/limit 60))))
                               (gen/stagger 1)
                               (cln/with-nemesis (:generator nemesis)))

               :model   (model/cas-register 0)
               :checker (checker/compose
                         {:perf   (checker/perf)
                          :details (independent/checker checker/linearizable) })
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
    (check [this test model history opts]
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

(defrecord SetsClient [tbl-created?]
  client/Client
  
  (setup! [this test node]
    (let [conn (init-conn node)]
      (info node "Connected")
      
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (info node "Creating table")
          (j/execute! @conn ["drop table if exists set"])
          (j/execute! @conn ["create table set (val int)"])))
      
      (assoc this :conn conn)))
  
  (invoke! [this test op]
    (let [conn (:conn this)]
      (with-txn op [c conn]
        (case (:f op)
          :add  (do
                  (j/insert! c :set {:val (:value op)})
                  (assoc op :type :ok))
          :read (->> (j/query c ["select val from set"])
                     (mapv :val)
                     (assoc op :type :ok, :value))
          ))))
  
  (teardown! [this test]
    (let [conn (:conn this)]
      (meh (with-timeout conn nil
             (j/execute! @conn ["drop table set"])))
      (close-conn @conn))
    ))


(defn sets-test
  [nodes nemesis linearizable]
  (basic-test nodes nemesis linearizable
              {:name        "set"
               :concurrency concurrency-factor
               :client      (SetsClient. (atom false))
               :generator   (gen/phases
                             (->> (range)
                                  (map (partial array-map
                                                :type :invoke
                                                :f :add
                                                :value))
                                  gen/seq
                                  (gen/stagger 1)
                                  (cln/with-nemesis (:generator nemesis)))
                             (gen/each
                              (->> {:type :invoke, :f :read, :value nil}
                                   (gen/limit 2)
                                   gen/clients)))
               :checker     (checker/compose
                             {:perf     (checker/perf)
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
  [linearizable]
  (reify checker/Checker
    (check [this test model history opts]
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
          {:valid?          (and (empty? lost) (empty? dups) (empty? off-order-sts)
                                 (or (not linearizable) (empty? off-order-val)))
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
            conn (init-conn node)]
        (info "Setting up client " n " for " (name node))

        (when (= node (jepsen/primary test))
          (j/execute! @conn ["drop table if exists mono"])
          (j/execute! @conn ["create table mono (val int, sts bigint, node int, tb int)"])
          (j/insert! @conn :mono {:val -1 :sts 0 :node -1 :tb -1}))

        (monotonic-client conn n)))

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
      (with-timeout conn nil
        (j/execute! @conn ["drop table if exists mono"]))
      (close-conn @conn))
    ))


(defn monotonic-test
  [nodes nemesis linearizable]
  (basic-test nodes nemesis linearizable
   {
    :name    "monotonic"
    :client (monotonic-client nil nil)
    :generator (gen/phases
                (->> (range)
                     (map (partial array-map
                                   :type :invoke
                                   :f :add
                                   :value))
                     gen/seq
                     (gen/stagger 1/10)
                     (cln/with-nemesis (:generator nemesis)))
                (gen/each
                 (->> {:type :invoke, :f :read, :value nil}
                      (gen/limit 2)
                      gen/clients)))
    :checker (checker/compose
              {:perf    (checker/perf)
               :details (check-monotonic linearizable)})
    }
   ))

;;;;;;;;;;;;;;;;;;;;;; Monotonic inserts over multiple tables ;;;;;;;;;;;;;;;;

(defn check-monotonic-split
  "Same as check-monotonic, but check ordering only from each client's perspective."
  [linearizable]
  (reify checker/Checker
    (check [this test model history opts]
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
                  sub-lists (->> (range multitable-spread)
                                 (map (fn [x] (->> final-read-l
                                                   (r/filter #(= x (nth % 3)))
                                                   (into ())
                                                   (reverse))))
                                 (into []))
                  off-order-pairs (map monotonic-order sub-lists)

                  off-order-sts (map first off-order-pairs)
                  off-order-val (map last off-order-pairs)

                  dups        (into [] (for [[id freq] (frequencies (into [] (map first final-read-l))) :when (> freq 1)] id))

                  ; Lost records are those we definitely added but weren't read
                  lost        (into [] (set/difference
                                        (into #{} (map first all-adds))
                                        (into #{} (map first final-read))))]
              {:valid?          (and (empty? lost)
                                     (empty? dups)
                                     (every? true? (into [] (map empty? off-order-sts)))
                                     (or (not linearizable)
                                         (every? true? (into [] (map empty? off-order-val)))))
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
      (let [n (if (= jdbc-mode :cdb-cluster) (str->int (subs (name node) 1 2)) 1)
            conn (init-conn node)]

        (info "Setting up client " n " for " (name node))

        (when (= node (jepsen/primary test))
          (dorun (for [x (range multitable-spread)]
                   (do
                     (j/execute! @conn [(str "drop table if exists mono" x)])
                     (j/execute! @conn [(str "create table mono" x
                                            " (val int, sts bigint, node int, tb int)")])
                     (j/insert! @conn (str "mono" x) {:val -1 :sts 0 :node -1 :tb x})
                     ))))

        (monotonic-multitable-client conn n)))


    (invoke! [this test op]
      (with-txn op [c conn]
        (case (:f op)
          :add  (let [rt (rand-int multitable-spread)
                      dbtime (db-time c)]
                  (j/insert! c (str "mono" rt) {:val (:value op) :sts dbtime :node nodenum :tb rt})
                  (assoc op :type :ok, :value [(:value op) dbtime nodenum rt]))

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
               (with-timeout conn nil
                 (j/execute! @conn [(str "drop table if exists mono" x)]))))
      (close-conn @conn))
))

(defn monotonic-multitable-test
  [nodes nemesis linearizable]
  (basic-test nodes nemesis linearizable
   {
    :name    "monotonic-multitable"
    :client (monotonic-multitable-client nil nil)
    :generator (gen/phases
                (->> (range)
                     (map (partial array-map
                                   :type :invoke
                                   :f :add
                                   :value))
                     gen/seq
                     (gen/stagger 1/10)
                     (cln/with-nemesis (:generator nemesis)))
                (gen/each
                 (->> {:type :invoke, :f :read, :value nil}
                      (gen/limit 2)
                      gen/clients)))
    :checker (checker/compose
              {:perf     (checker/perf)
               :details  (check-monotonic-split linearizable)})
    }
   ))

;;;;;;;;;;;;;;;;;;;;;;;;;;; Test for transfers between bank accounts ;;;;;;;;;;;;;;;;;;;;;;;

(defrecord BankClient [conn n starting-balance]
  client/Client
    (setup! [this test node]
      (let [conn (init-conn node)]

        (when (= node (jepsen/primary test))
          ;; Create initial accounts.
          (j/execute! @conn ["drop table if exists accounts"])
          (j/execute! @conn ["create table accounts (id int not null primary key, balance bigint not null)"])
          (dotimes [i n]
            (j/insert! @conn :accounts {:id i :balance starting-balance})))

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
      (with-timeout conn nil
        (j/execute! @conn ["drop table if exists accounts"]))
      (close-conn @conn))
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
             :amount (+ 1 (rand-int 5))}}))

(def bank-diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              bank-transfer))

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test model history opts]
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
                                          :op       op}

                                         (some neg? balances)
                                         {:type     :negative-value
                                          :found    balances
                                          :op       op}
                                         ))))
                           (r/filter identity)
                           (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defn bank-test
  [nodes nemesis linearizable]
  (basic-test nodes nemesis linearizable
    {:name "bank"
     ;:concurrency 20
     :model  {:n 4 :total 40}
     :client (bank-client 4 10)
     :generator (gen/phases
                  (->> (gen/mix [bank-read bank-diff-transfer])
                       (gen/clients)
                       (gen/stagger 1/10)
                       (cln/with-nemesis (:generator nemesis)))
                  (gen/clients (gen/once bank-read)))
     :checker (checker/compose
                {:perf (checker/perf)
                 :details (bank-checker)})}))

