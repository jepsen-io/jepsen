
(ns jepsen.cockroach
    "Tests for CockroachDB"
    (:require [clojure.tools.logging :refer :all]
            [clj-ssh.ssh :as ssh]
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
             [model :as model]
             [checker :as checker]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :refer [timeout]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.ubuntu :as ubuntu]))

(defn eval!
  "Evals a sql string from the command line."
  [s]
  (c/exec "/home/ubuntu/sql.sh" s))

(defn setup-db!
  "Set up the jepsen database in the cluster."
  [node]
  (eval! "drop database if exists jepsen;")
  (eval! "create database jepsen;")
  (eval! "create table jepsen.test (name string, val int, primary key (name));")
  (eval! "insert into jepsen.test values ('a', 0);")
  (eval! "create table jepsen.set (val int);")
  (eval! "create table if not exists jepsen.accounts (id int not null primary key, balance bigint not null);")
  )

(defn stop!
  "Remove the jepsen database from the cluster."
  [node]
  (eval! "drop database if exists jepsen;"))

(def log-files
  ["/home/ubuntu/logs/cockroach.stderr"])

(defn str->int [str]
  (let [n (read-string str)]
    (if (number? n) n nil)))

(defn db
  "Sets up and tears down CockroachDB."
  []
  (reify db/DB
    (setup! [_ test node]

      (c/exec "/home/ubuntu/restart.sh")
      (jepsen/synchronize test)
      (if (= node (jepsen/primary test)) (setup-db! node))

      (info node "Setup complete")
      (Thread/sleep 1000))

    (teardown! [_ test node]

      (if (= node (jepsen/primary test)) (stop! node))
      (jepsen/synchronize test)
      (apply c/exec :truncate :-c :--size 0 log-files)
      )
    
    db/LogFiles
    (log-files [_ test node] log-files)))

; -------------------- Accessing CockroachDB via SQL-over-SSH -----------------

(defn ssh-open
  "Open a SSH session to the given node."
  [node]
  (let [agent (ssh/ssh-agent {})
        host (name node)
        conn (ssh/session agent host {:username "ubuntu" :strict-host-key-checking :no})]
    conn))

(defn ssh-sql-sh
  "Evals a sql string from the command line over an existing SSH connection."
  [conn s]
  (ssh/ssh conn {:cmd (str "/home/ubuntu/sql.sh " s)}))

(defn ssh-sql
    "Evals a SQL string and return the error status and messages if any"
  [conn stmts]
  (let [res (ssh-sql-sh conn stmts)
        out (str/split-lines (str/trim-newline (:out res)))
        err (str "sql error: " (str/join " " out))]
    [res out err]))
  

;-------------------- Common definitions ------------------------------------

(defn basic-test
  [nodes opts]
  (merge tests/noop-test
         {:nodes   nodes
          :name    (str "cockroachdb " (:name opts))
          :os      ubuntu/os
          :db      (db)
          :nemesis (nemesis/partition-random-halves)}
         (dissoc opts :name)))

                                        ;
;-------------------- Test for an atomic counter ----------------------------

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn ssh-cnt-client
  "A client for a single compare-and-set register, using sql cli over ssh"
  [conn]
  (reify client/Client
    (setup! [_ test node]
      (ssh-cnt-client (ssh-open node)))
    
    (invoke! [this test op]
      (timeout 2000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read (let [[res out err]
                             (ssh-sql conn (str "begin "
                                                "\"set transaction isolation level serializable\" "
                                                "\"select val from jepsen.test\"  "
                                                "commit"))]
                         (if (zero? (:exit res))
                           (assoc op :type :ok, :value (str->int (nth out 4)))
                           (assoc op :type :info, :error err)))
                 :write (let [[res out err] (ssh-sql conn (str "begin "
                                                               "\"set transaction isolation level serializable\" "
                                                               "\"update jepsen.test set val=" (:value op) " where name='a'\" "
                                                               "commit"))]
                          (if (and (zero? (:exit res)) (= (last out) "OK"))
                            (assoc op :type :ok)
                            (assoc op :type :info, :error err)))
                 :cas (let [[value' value] (:value op)
                            [res out err] (ssh-sql conn (str "begin "
                                                             "\"set transaction isolation level serializable\" "
                                                             "\"select val from jepsen.test\" "
                                                             "\"update jepsen.test set val=" value " where name='a' and val=" value' "\" "
                                                             "\"select val from jepsen.test\" "
                                                             " commit"))]
                        (if (zero? (:exit res))
                          (assoc op 
                                 :type (if (and (= (last out) "OK")
                                                (= (str->int (nth out 4)) value')
                                                (= (str->int (nth out 8)) value))
                                         :ok :fail),
                                 :error err)
                          (assoc op :type :info, :error err)))
                 ))
      )
      

    (teardown! [_ test]
    	   (ssh/disconnect conn))
    ))

(defn atomic-test
  [nodes]
  (basic-test nodes
   {
    :name    "atomic"
    :client  (ssh-cnt-client nil)
    :generator (->> (gen/mix [r w cas])
                    (gen/stagger 1)
                    (gen/nemesis
                     (gen/seq (cycle [(gen/sleep 5)
                                      {:type :info, :f :start}
                                      (gen/sleep 5)
                                      {:type :info, :f :stop}])))
                    (gen/time-limit 30))
    :model   (model/cas-register 0)
    :checker (checker/compose
              {:perf   (checker/perf)
               :linear checker/linearizable})
    }
   ))

;-------------------- Test for a distributed set ----------------------------


(defn ssh-set-client
  [conn]
  (reify client/Client
    (setup! [_ test node]
      (ssh-set-client (ssh-open node)))
      
    (invoke! [this test op]
      (case (:f op)
        :add  (let [[res out err] (ssh-sql conn (str "begin "
                                                     "\"set transaction isolation level serializable\" "
                                                     "\"insert into jepsen.set values (" (:value op) ")\"  "
                                                     "commit"))]
                (if (zero? (:exit res))
                  (assoc op :type :ok)
                  (assoc op :type :fail, , :error err)))
        :read (let [[res out err] (ssh-sql conn (str "begin "
                                                     "\"set transaction isolation level serializable\" "
                                                     "\"select val from jepsen.set\"  "
                                                     "commit"))]
                (if (zero? (:exit res))
                  (assoc op :type :ok, :value (into (sorted-set) (map str->int (drop-last 1 (drop 4 out)))))
                  (assoc op :type :fail, :error err)))
        ))
    
    (teardown! [_ test]
      (ssh/disconnect conn))
    ))

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
                              {:type :info, :f :stop}
                              ])))
           (gen/time-limit 30))
      (gen/nemesis (gen/once {:type :info, :f :stop}))
      (gen/sleep 5))))


(defn sets-test
  [nodes]
  (basic-test nodes
   {
    :name    "set"
    :client (ssh-set-client nil)
    :generator (gen/phases
                (->> (range)
                     (map (partial array-map
                                   :type :invoke
                                   :f :add
                                   :value))
                     gen/seq
                     (gen/delay 1/10)
                     with-nemesis)
                (->> {:type :invoke, :f :read, :value nil}
                     gen/once
                     gen/clients))
    :checker (checker/compose
              {:perf (checker/perf)
               :set  checker/set})
    }
   ))

; --------------------------- Test for transfers between bank accounts -------------------

(defrecord SSHBankClient [conn n starting-balance]
  client/Client
    (setup! [this test node]
      (let [conn (ssh-open node)]
        (if (= node (jepsen/primary test))
                                        ; Create initial accts
          (dotimes [i n]
            (let [[res out err] (ssh-sql conn (str "\"insert into jepsen.accounts values (" i "," starting-balance ")\""))]
              (println "Created account " i " (" (:exit res) ", " err ")"))))
        (assoc this :conn conn))
      )

    (invoke! [this test op]
      (timeout 2000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read (let [[res out err] (ssh-sql conn (str "begin "
                                                              "\"set transaction isolation level serializable\" "
                                                              "\"select balance from jepsen.accounts\" "
                                                              "commit"))]
                         (if (zero? (:exit res))
                           (assoc op :type :ok, :value (map str->int (drop-last 1 (drop 4 out))))
                           (assoc op :type :fail, :error err)))

                 :transfer
                 (let [{:keys [from to amount]} (:value op)
                       [res out err] (ssh-sql conn (str "begin "
                                                        "\"set transaction isolation level serializable\" "
                                                        "\"select balance-" amount " from jepsen.accounts where id = " from "\" "
                                                        "\"select balance+" amount " from jepsen.accounts where id = " to "\" "
                                                        "\"update jepsen.accounts set balance=balance-" amount " where id = " from "\"  "
                                                        "\"update jepsen.accounts set balance=balance+" amount " where id = " to "\"  "
                                                        "commit"))]
                   (if (zero? (:exit res))
                     (assoc op :type :ok)
                     (assoc op :type :fail, :error err)))
                 )))

    (teardown! [_ test]
      (ssh/disconnect conn))
    )
  
(defn ssh-bank-client
  "Simulates bank account transfers between n accounts, each starting with
  starting-balance."
  [n starting-balance]
  (SSHBankClient. nil n starting-balance))

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
     :client (ssh-bank-client n initial-balance)
     :generator (gen/phases
                  (->> (gen/mix [bank-read bank-diff-transfer])
                       (gen/clients)
                       (gen/stagger 1/10)
                       (gen/nemesis
                        (gen/seq (cycle [(gen/sleep 5)
                                         {:type :info, :f :start}
                                         (gen/sleep 5)
                                         {:type :info, :f :stop}])))
                       (gen/time-limit 30))
                  (gen/log "waiting for quiescence")
                  (gen/sleep 30)
                  (gen/clients (gen/once bank-read)))
     :checker (checker/compose
                {:perf (checker/perf)
                 :bank (bank-checker)})}))
