(ns jepsen.postgres-rds
  "Tests for Postgres RDS"
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
            [clojure.java.jdbc :as j]))

(defn open-conn?
  "Is this connection open? e.g. does it have a :connection key?"
  [conn]
  (boolean (:connection conn)))

(defn open-conn
  "Given a JDBC connection spec, opens a new connection unless one already
  exists. JDBC represents open connections as a map with a :connection key.
  Won't open if a connection is already open."
  [spec]
  (if (:connection spec)
    spec
    (j/add-connection spec (j/get-connection spec))))

(defn close-conn
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (:connection conn)]
    (.close c))
  (dissoc conn :connection))

(defmacro with-conn
  "So here's the deal: we need to hold connections open to re-use them, but we
  can't hold them open *forever* or we won't track failovers in stuff like
  Postgres RDS. So instead we'll have an atom that can refer to either a
  connection *spec*, or a full connection. open-conn and close-conn let us
  transform one into the other. This macro takes that atom and binds ai
  connection for the duration of its body, automatically reconnecting on any
  exception.

  Not re-entrant. Probably full of concurrency bugs. I dunno, this is a gross
  hack."
  [[conn-sym conn-atom] & body]
  `(let [~conn-sym (locking ~conn-atom
                     (swap! ~conn-atom open-conn))]
     (try
       ~@body
       (catch Throwable t#
         ; Reopen
         (warn "Lost connection" ~conn-sym ", reconnecting")
         (locking ~conn-atom
           (swap! ~conn-atom (comp open-conn close-conn)))
         (throw t#)))))

(def galera-rollback-msg
  "mariadb drivers have a few exception classes that use this message"
  "Deadlock found when trying to get lock; try restarting transaction")

(defmacro capture-txn-abort
  "Converts aborted transactions to an ::abort keyword"
  [& body]
  `(try ~@body
        ; Galera
        (catch java.sql.SQLTransactionRollbackException e#
          (if (= (.getMessage e#) galera-rollback-msg)
            ::abort
            (throw e#)))
        (catch java.sql.BatchUpdateException e#
          (let [m# (.getMessage e#)]
            (cond ; Galera
                  (= m# galera-rollback-msg)
                  ::abort

                  ; Postgres
                  (re-find #"Batch entry .+ was aborted" m#)
                  ::abort

                  true
                  (throw e#))))))

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
        ; MariaDB
        (catch java.sql.SQLNonTransientConnectionException e#
          (condp = (.getMessage e#)
            "WSREP has not yet prepared node for application use"
            (assoc ~op :type :fail, :value (.getMessage e#))

            (throw e#)))))

(defmacro with-txn
  "Executes body in a transaction, with a timeout, automatically retrying
  conflicts and handling common errors."
  [op [c conn-atom] & body]
  `(timeout 5000 (assoc ~op :type :info, :value :timed-out)
            (with-conn [c# ~conn-atom]
              (j/with-db-transaction [~c c# :isolation :serializable]
                (with-error-handling ~op
                  (with-txn-retries
                    ~@body))))))

(defrecord BankClient [conn-spec
                       conn
                       node
                       n
                       starting-balance
                       lock-type
                       in-place?]
  client/Client
  (setup! [this test node]
    (let [conn (atom (conn-spec node))]
      (with-conn [c conn]
        ; Create table
        (j/execute! c ["create table if not exists accounts
                       (id      int not null primary key,
                       balance bigint not null)"])

        ; Create initial accts
        (dotimes [i n]
          (try
            (with-txn-retries
              (j/insert! c :accounts {:id i, :balance starting-balance}))
            (catch java.sql.SQLIntegrityConstraintViolationException e nil)
            (catch org.postgresql.util.PSQLException e
              (if (re-find #"duplicate key value violates unique constraint"
                           (.getMessage e))
                nil
                (throw e)))))))

    (assoc this :node node, :conn (atom (conn-spec node))))

  (invoke! [this test op]
    (with-txn op [c conn]
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
  [conn-spec n starting-balance lock-type in-place?]
  (map->BankClient {:conn-spec conn-spec
                    :n n
                    :starting-balance starting-balance
                    :lock-type lock-type
                    :in-place? in-place?}))

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

(defn basic-test
  [opts]
  (merge tests/noop-test
         {:name (str "postgres rds " (:name opts))
          :nodes []}
         (dissoc opts :name)))

(defn bank-test
  [node n initial-balance lock-type in-place?]
  (basic-test
    {:name "bank"
     :concurrency 10
     :model  {:n n :total (* n initial-balance)}
     :client (bank-client (fn conn-spec [_]
                            ; We ignore the nodes here and just use the AWS node
                            {:classname   "org.postgresql.Driver"
                             :subprotocol "postgresql"
                             :subname     (str "//" (name node) ":5432/jepsen")
                             :user        "jepsen"
                             :password    "jepsenpw"})
                            n initial-balance lock-type in-place?)
     :generator (gen/phases
                  (->> (gen/mix [bank-read bank-diff-transfer])
                       (gen/clients)
                       (gen/stagger 1/10)
                       (gen/time-limit 20))
                  (gen/log "waiting for quiescence")
                  (gen/sleep 10)
                  (gen/clients (gen/once bank-read)))
     :nemesis nemesis/noop
     :checker (checker/compose
                {:perf (checker/perf)
                 :bank (bank-checker)})}))
