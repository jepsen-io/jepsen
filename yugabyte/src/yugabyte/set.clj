(ns yugabyte.set
  "Adds elements to sets and reads them back"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]
                    [checker :as checker]]
            [clojurewerkz.cassaforte [client :as cass]
                                     [query :as q]
                                     [cql :as cql]]
            [yugabyte [client :as c]
                      [core :refer [yugabyte-test]]]))

(def setup-lock (Object.))
(def keyspace "jepsen")
(def table "elements")

(c/defclient CQLSetClient keyspace []
  (setup! [this test]
          (c/create-table conn table
                          (q/if-not-exists)
                          (q/column-definitions {:val :int
                                                 :count :counter
                                                 :primary-key [:val]})))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :add (do (cql/update conn table
                             {:count (q/increment)}
                             (q/where {:val (:value op)}))
                 (assoc op :type :ok))

        :read (->> (cql/select-with-ks conn keyspace table)
                   (mapcat (fn [row]
                             (info row)
                             (repeat (:count row) (:val row))))
                   ; sort
                   (sort)
                   (assoc op :type :ok, :value)))))

  (teardown! [this test]))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn test
  [opts]
  (yugabyte-test
    (merge opts
           {:name "set"
            :client (->CQLSetClient)
            :client-generator (->> (gen/reserve (/ (:concurrency opts) 2) (adds)
                                                (reads))
                                   (gen/stagger 1/10))
            :checker (checker/compose {:perf (checker/perf)
                                       :set (checker/set-full)})})))
