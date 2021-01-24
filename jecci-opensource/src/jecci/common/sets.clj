(ns jecci.common.sets
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [knossos.op :as op]
            [jecci.common.basic :as basic]
            [jecci.interface.client :as ic]))

(defrecord Adds [x]
  gen/Generator
  (op [this test ctx]
    [(gen/fill-in-op {:type :invoke, :f :add, :value x} ctx) (Adds. (inc x))])
  (update [this test ctx event]
    this))

(defn reads
  []
  (gen/repeat {:type :invoke, :f :read, :value nil}))

(defn workload
  [opts]
  (let [c (:concurrency opts)]
    {:client (ic/gen-SetClient nil)
     :generator (->> (gen/reserve (/ c 2) (Adds. 0) (reads))
                     (gen/stagger 1/10))
     :checker (checker/set-full)}))

(defn cas-workload
  [opts]
  (assoc (workload opts) :client (ic/gen-CasSetClient nil)))

