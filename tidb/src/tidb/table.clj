(ns tidb.table
  "A test for table creation"
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen
             [client :as client]
             [generator :as gen]
             [checker :as checker]]
            [jepsen.tests.bank :as bank]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]
            [clojure.tools.logging :refer :all]))

(defrecord TableClient [conn last-created-table]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :create-table
      (do (c/execute! conn [(str "create table if not exists t"
                                 (:value op)
                                 " (id int not null primary key, val int)")])
          (swap! last-created-table (fn [x x'] (if (nil? x)
                                                 x'
                                                 (max x x')))
                 (:value op))
          (assoc op :type :ok))

      :insert
      (try
        (let [[table k] (:value op)]
          (c/insert! conn (str "t" table) {:id k})
          (assoc op :type :ok))
        (catch java.sql.SQLSyntaxErrorException e
          (condp re-find (.getMessage e)
            #"Table .* doesn't exist"
            (assoc op :type :fail, :error :doesn't-exist)

            (throw e)))
        (catch java.sql.SQLIntegrityConstraintViolationException e
          (assoc op :type :fail, :error [:duplicate-key (.getMessage e)])))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn create-then-insert
  "A generator for two operations: creating a table, and inserting a value into
  it."
  [table-id]
  (gen/seq [{:type :invoke, :f :create-table, :value table-id}
            {:type :invoke, :f :insert, :value [table-id 0]}]))

(defn generator
  "Repeatedly create and insert into new tables."
  [last-created-table]
  (let [next-create (atom 0)]
    (fn gen [_ _]
      (if (and (< (rand) 0.8) @last-created-table)
        {:type :invoke, :f :insert,       :value [@last-created-table 0]}
        {:type :invoke, :f :create-table, :value (swap! next-create inc)}))))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [bad (filter (fn [op] (and (= :fail (:type op))
                                     (= :doesn't-exist (:error op)))) history)]
        {:valid? (not (seq bad))
         :errors bad}))))

(defn workload
  [opts]
  (let [last-created-table (atom nil)]
    {:client    (TableClient. nil last-created-table)
     :generator (generator last-created-table)
     :checker   (checker)}))
