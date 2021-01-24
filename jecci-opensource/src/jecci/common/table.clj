(ns jecci.common.table
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
            [jecci.common.basic :as basic]
            [clojure.tools.logging :refer :all]
            [jecci.interface.client :as ic]))


(defrecord Table [last-created-table next-create]
  gen/Generator
  (op [this test ctx]
    (if (and (< (rand) 0.8) @last-created-table)
        [(gen/fill-in-op {:type :invoke, :f :insert,       :value [@last-created-table 0]} ctx) this]
        [(gen/fill-in-op {:type :invoke, :f :create-table, :value (swap! next-create inc)} ctx) this]))
  (update [this test ctx event]
    this))

(defn table [last-created-table]
  (Table. last-created-table (atom 0)))

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
    {:client    (ic/gen-TableClient nil last-created-table)
     :generator (table last-created-table)
     :checker   (checker)}))
