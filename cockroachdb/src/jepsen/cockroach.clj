(ns jepsen.cockroach
    "Tests for CockroachDB"
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
            [jepsen.os.ubuntu :as ubuntu]
            [clojure.java.jdbc :as j]))

(defn eval!
  "Evals a mysql string from the command line."
  [s]
  (c/exec "/home/ubuntu/sql.sh" s))

(defn setup-db!
  "Adds a jepsen database to the cluster."
  [node]
  (eval! "create database if not exists jepsen;")
  (eval! "create table if not exists jepsen.test (name char(1), val int);")
  (eval! "insert into jepsen.test values ('a', 0);")
  )

(defn stop!
  "Remove the jepsen database from the cluster."
  [node]
  (eval! "drop database if exists jepsen;"))

(def log-files
  ["/home/ubuntu/logs/cockroach.stderr"])


(defn db
  "Sets up and tears down Galera."
  [version]
  (reify db/DB
    (setup! [_ test node]

      (c/exec "/home/ubuntu/restart.sh")
      (jepsen/synchronize test)
      (setup-db! node)

      (info node "Install complete")
      (Thread/sleep 5000))

    (teardown! [_ test node]

        (stop! node)
        (apply c/exec :truncate :-c :--size 0 log-files)
	)
    
    db/LogFiles
    (log-files [_ test node] log-files)))

(defn conn-spec
  "jdbc connection spec for a node."
  [node]
  {:classname   "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname     (str "//" (name node) ":15432/jepsen")
   :user        "psql"
   :password    "dummy"})

(defmacro with-txn
  "Executes body in a transaction, with a timeout"
  [op [c conn-atom] & body]
  `(timeout 5000 (assoc ~op :type :info, :value :timed-out)
            (with-conn [c# ~conn-atom]
              (j/with-db-transaction [~c c# :isolation :serializable]
                 ~@body))))

(defn simple-test
  [version]
  (assoc tests/noop-test
         :name "cockroachdb"
	 :os ubuntu/os
	 :db (db version)
	 ))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn client
  "A client for a single compare-and-set register"
  [conn]
  (reify client/Client
    (setup! [_ test node]
      (let [spec (conn-spec node)
            conn (j/add-connection spec (j/get-connection spec))
            ]
      (client conn)))

    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read (j/with-db-transaction [c conn :isolation :serializable]
                         (->>
                          (j/query c "select val from test")
                          (mapv :val)
                          (assoc op :type :ok, :value)))
                 :write (do
                          (j/with-db-transaction [c conn :isolation :serializable]
                            (j/update! c :test {:val (:value op)} ["name='a'"]))
                          (assoc op :type :ok))
                 :cas nil  
                 ))
      )
      

    (teardown! [_ test]
               (.close conn))
    ))

(defn client-test
  [version]
  (assoc tests/noop-test
         :name    "cockroachdb"
         :os      ubuntu/os
         :db      (db version)
         :client  (client nil)
         :generator (->> (gen/mix [r w])
                         (gen/stagger 1)
                         (gen/clients)
                         (gen/time-limit 15))
))
