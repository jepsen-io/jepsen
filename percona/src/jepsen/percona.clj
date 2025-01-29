(ns jepsen.percona
  "Tests for Percona XtraDB"
  (:require [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [knossos.op :as op]
            [jepsen [client :as client]
             [core :as jepsen]
             [db :as db]
             [tests :as tests]
             [control :as c :refer [|]]
             [checker :as checker]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :refer [timeout meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]
            [clojure.java.jdbc :as j]
            [honeysql [core :as sql]
                      [helpers :as h]]))

(def log-files
  ["/var/log/syslog"
   "/var/log/mysql.log"
   "/var/log/mysql.err"
   "/var/lib/mysql/queries.log"])

(def dir "/var/lib/mysql")
(def stock-dir "/var/lib/mysql-stock")

(defn install!
  "Downloads and installs the percona packages."
  [node version]
  (debian/add-repo!
    :percona
    "deb http://repo.percona.com/apt jessie main"
    "keys.gnupg.net"
    "1C4CBDCDCD2EFD2A")

  (c/su
    (c/exec :echo (slurp (io/resource "apt-prefs"))
            :> "/etc/apt/preferences.d/00percona.pref")

    (debian/install [:rsync])

    (when-not (= version (debian/installed-version :percona-xtradb-cluster-56))

      ; dpkg is gonna ask us some questions
      (doseq [line ["percona-xtradb-cluster-56 mysql-server/root_password password jepsen"
                    "percona-xtradb-cluster-56 mysql-server/root_password_again password jepsen"
                    "percona-xtradb-cluster-56 mysql-server-5.1/start_on_boot boolean false"
                    "percona-xtradb-cluster-server-5.6 percona-xtradb-cluster-server/root_password_again password jepsen"
                    "percona-xtradb-cluster-server-5.6 percona-xtradb-cluster-server/root_password password jepsen"]]
        (c/exec :echo line | :debconf-set-selections))

      ; Don't let our config fuck up initial startup
      (c/exec :rm :-rf "/etc/mysql/conf.d/jepsen.cnf")

      ; Don't preserve old dir
      (c/exec :rm :-rf dir)

      (debian/install {:percona-xtradb-cluster-56 version})

      (c/exec :service :mysql :stop)

      ; Squirrel away a copy of the data files
      (c/exec :rm :-rf stock-dir)
      (c/exec :cp :-rp dir stock-dir))))

(defn cluster-address
  "Connection string for a test."
  [test node]
  (if (= node (jepsen/primary test))
    "gcomm://"
    (str "gcomm://" (str/join "," (map name (:nodes test))))))

(defn configure!
  "Sets up config files"
  [test node]
  (c/su
    ; my.cnf
    (c/exec :echo (-> (io/resource "jepsen.cnf")
                      slurp
                      (str/replace #"%CLUSTER_ADDRESS%"
                                   (cluster-address test node)))
            :> "/etc/mysql/conf.d/jepsen.cnf")))

(defn stop!
  "Stops sql daemon."
  [node]
  (info node "stopping mysqld")
  (meh (cu/grepkill "mysqld")))

(defn eval!
  "Evals a mysql string from the command line."
  [s]
  (c/exec :mysql :-u "root" "--password=jepsen" :-e s))

(defn conn-spec
  "jdbc connection spec for a node."
  [node]
  {:classname   "org.mariadb.jdbc.Driver"
   :subprotocol "mariadb"
   :subname     (str "//" (name node) ":3306/jepsen")
   :user        "jepsen"
   :password    "jepsen"})

(defn setup-db!
  "Adds a jepsen database to the cluster."
  [node]
  (eval! "create database if not exists jepsen;")
  (eval! (str "GRANT ALL PRIVILEGES ON jepsen.* "
              "TO 'jepsen'@'%' IDENTIFIED BY 'jepsen';")))

(defn db
  "Sets up and tears down Galera."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (install! node version)
      (configure! test node)

      (when (= node (jepsen/primary test))
        (c/su (c/exec :service :mysql :start :bootstrap-pxc)))

      (jepsen/synchronize test)
      (when (not= node (jepsen/primary test))
        (c/su (c/exec :service :mysql :start)))

      (jepsen/synchronize test)
      (setup-db! node)

      (info node "Install complete")
      (Thread/sleep 5000))

    (teardown! [_ test node]
      (c/su
        (stop! node)
        (apply c/exec :truncate :-c :--size 0 log-files))
        (c/exec :rm :-rf dir)
        (c/exec :cp :-rp stock-dir dir))

    db/LogFiles
    (log-files [_ test node] log-files)))

(def rollback-msg
  "mariadb drivers have a few exception classes that use this message"
  "Deadlock found when trying to get lock; try restarting transaction")

(defmacro capture-txn-abort
  "Converts aborted transactions to an ::abort keyword"
  [& body]
  `(try ~@body
        (catch java.sql.SQLTransactionRollbackException e#
          (if (= (.getMessage e#) rollback-msg)
            ::abort
            (throw e#)))
        (catch java.sql.BatchUpdateException e#
          (if (= (.getMessage e#) rollback-msg)
            ::abort
            (throw e#)))))

(defmacro with-txn-retries
  "Retries body on rollbacks."
  [& body]
  `(loop []
     (let [res# (capture-txn-abort ~@body)]
       (if (= ::abort res#)
         (recur)
         res#))))

(defmacro with-txn-aborts
  "Aborts body on rollbacks."
  [op & body]
  `(let [res# (capture-txn-abort ~@body)]
     (if (= ::abort res#)
       (assoc ~op :type :fail)
       res#)))

(defmacro with-error-handling
  "Common error handling for Galera errors"
  [op & body]
  `(try ~@body
        (catch java.sql.SQLNonTransientConnectionException e#
          (condp = (.getMessage e#)
            "WSREP has not yet prepared node for application use"
            (assoc ~op :type :fail, :value (.getMessage e#))

            (throw e#)))))

(defmacro with-txn
  "Executes body in a transaction, with a timeout, automatically retrying
  conflicts and handling common errors."
  [op [c node] & body]
  `(timeout 5000 (assoc ~op :type :info, :value :timed-out)
           (with-error-handling ~op
             (with-txn-retries
               (j/with-db-transaction [~c (conn-spec ~node)
                                       :isolation :serializable]
                 (j/execute! ~c ["start transaction with consistent snapshot"])
                 ~@body)))))

(defn basic-test
  [opts]
  (merge tests/noop-test
         {:name (str "percona " (:name opts))
          :os   debian/os
          :db   (db (:version opts))
          :nemesis (nemesis/partition-random-halves)}
         (dissoc opts :name :version)))

(defn with-nemesis
  "Wraps a client generator in a nemesis that induces failures and eventually
  stops."
  [client]
  (gen/phases
    (gen/phases
      (->> client
           (gen/nemesis
             (gen/seq (cycle [(gen/sleep 0)
                              {:type :info, :f :start}
                              (gen/sleep 10)
                              {:type :info, :f :stop}])))
           (gen/time-limit 30))
      (gen/nemesis (gen/once {:type :info, :f :stop}))
      (gen/sleep 5))))

(defrecord BankClient [node n starting-balance lock-type in-place?]
  client/Client
  (setup! [this test node]
    (j/with-db-connection [c (conn-spec (first (:nodes test)))]
      ; Create table
      (j/execute! c ["create table if not exists accounts
                     (id      int not null primary key,
                     balance bigint not null)"])
      ; Create initial accts
      (dotimes [i n]
        (try
          (with-txn-retries
            (j/insert! c :accounts {:id i, :balance starting-balance}))
          (catch java.sql.SQLIntegrityConstraintViolationException e nil))))

    (assoc this :node node))

  (invoke! [this test op]
    (with-txn op [c (first (:nodes test))]
      (try
        (case (:f op)
          :read (->> (j/query c [(str "select * from accounts" lock-type)])
                     (mapv :balance)
                     (assoc op :type :ok, :value))

          :transfer
          (let [{:keys [from to amount]} (:value op)
                b1 (-> c
                       (j/query [(str "select * from accounts where id = ?"
                                      lock-type)
                                 from]
                         :row-fn :balance)
                       first
                       (- amount))
                b2 (-> c
                       (j/query [(str "select * from accounts where id = ?"
                                      lock-type)
                                 to]
                         :row-fn :balance)
                       first
                       (+ amount))]
            (cond (neg? b1)
                  (assoc op :type :fail, :value [:negative from b1])

                  (neg? b2)
                  (assoc op :type :fail, :value [:negative to b2])

                  true
                  (if in-place?
                    (do (j/execute! c ["update accounts set balance = balance - ? where id = ?" amount from])
                        (j/execute! c ["update accounts set balance = balance + ? where id = ?" amount to])
                        (assoc op :type :ok))
                    (do (j/update! c :accounts {:balance b1} ["id = ?" from])
                        (j/update! c :accounts {:balance b2} ["id = ?" to])
                        (assoc op :type :ok)))))))))

  (teardown! [_ test]))

(defn bank-client
  "Simulates bank account transfers between n accounts, each starting with
  starting-balance."
  [n starting-balance lock-type in-place?]
  (BankClient. nil n starting-balance lock-type in-place?))

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
  [version n initial-balance lock-type in-place?]
  (basic-test
    {:name "bank"
     :concurrency 20
     :version version
     :model  {:n n :total (* n initial-balance)}
     :client (bank-client n initial-balance lock-type in-place?)
     :generator (gen/phases
                  (->> (gen/mix [bank-read bank-diff-transfer])
                       (gen/clients)
                       (gen/stagger 1/10)
                       (gen/time-limit 100))
                  (gen/log "waiting for quiescence")
                  (gen/sleep 10)
                  (gen/clients (gen/once bank-read)))
     :nemesis nemesis/noop
     :checker (checker/compose
                {:perf (checker/perf)
                 :bank (bank-checker)})}))
