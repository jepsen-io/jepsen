(ns jepsen.galera.dirty-reads
  "Dirty read analysis for Mariadb Galera Cluster.

  In this test, writers compete to set every row in a table to some unique
  value. Concurrently, readers attempt to read every row. We're looking for
  casdes where a *failed* transaction's number was visible to some reader."
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
             [util :refer [timeout meh]]
             [galera :as galera]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]
            [clojure.java.jdbc :as j]))


(defrecord Client [node n]
  client/Client
  (setup! [this test node]
    (j/with-db-connection [c (galera/conn-spec node)]
      ; Create table
      (j/execute! c ["create table if not exists dirty
                     (id      int not null primary key,
                      x       bigint not null)"])
      ; Create rows
      (dotimes [i n]
        (try
          (galera/with-txn-retries
            (Thread/sleep (rand-int 10))
            (j/insert! c :dirty {:id i, :x -1}))
          (catch java.sql.SQLIntegrityConstraintViolationException e nil))))

    (assoc this :node node))

  (invoke! [this test op]
    (timeout 5000 (assoc ~op :type :info, :value :timed-out)
      (galera/with-error-handling op
        (galera/with-txn-aborts op
          (j/with-db-transaction [c (galera/conn-spec node)
                                  :isolation :serializable]
            (try
              (case (:f op)
                :read (->> (j/query c ["select * from dirty"])
                           (mapv :x)
                           (assoc op :type :ok, :value))

                :write (let [x (:value op)
                             order (shuffle (range n))]
                         (doseq [i order]
                           (j/query c ["select * from dirty where id = ?" i]))
                         (doseq [i order]
                           (j/update! c :dirty {:x x} ["id = ?" i]))
                         (assoc op :type :ok)))))))))

  (teardown! [_ test]))

(defn client
  [n]
  (Client. nil n))

(defn checker
  "We're looking for a failed transaction whose value became visible to some
  read."
  []
  (reify checker/Checker
    (check [this test model history]
      (let [failed-writes (->> history
                               (r/filter op/fail?)
                               (r/filter #(= :write (:f %)))
                               (r/map :value)
                               (into (hash-set)))
            reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %)))
                       (r/map :value))
            inconsistent-reads (->> reads
                                    (r/filter (partial apply not=))
                                    (into []))
            filthy-reads (->> reads
                              (r/filter (partial some failed-writes))
                              (into []))]
        {:valid? (empty? filthy-reads)
         :inconsistent-reads inconsistent-reads
         :dirty-reads filthy-reads}))))

(def reads {:type :invoke, :f :read, :value nil})

(def writes (->> (range)
                 (map (partial array-map
                               :type :invoke,
                               :f :write,
                               :value))
                 gen/seq))

(defn test-
  [version n]
  (galera/basic-test
    {:name "dirty reads"
     :concurrency 50
     :version version
     :client (client n)
     :generator (->> (gen/mix [reads writes])
                     gen/clients
                     (gen/time-limit 1000))
     :nemesis nemesis/noop
     :checker (checker/compose
                {:perf (checker/perf)
                 :dirty-reads (checker)})}))
