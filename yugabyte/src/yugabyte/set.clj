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
                             (repeat (:count row) (:val row))))
                   sort
                   (assoc op :type :ok, :value)))))

  (teardown! [this test]))

(def group-count
  "Number of distinct groups for indexing"
  8)

(c/defclient CQLSetIndexClient keyspace []
  (setup! [this test]
          (c/create-transactional-table conn table
                                        (q/if-not-exists)
                                        (q/column-definitions
                                          {:val         :int
                                           :grp         :int
                                           :primary-key [:val]}))
          (c/create-index conn "elements_by_group"
                            (q/on-table table)
                            (q/and-column :grp)))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :add (do (cql/update conn table
                             {:grp (rand-int group-count)}
                             (q/where {:val (:value op)}))
                 (assoc op :type :ok))

         :read (->> (cql/select conn table
                               (q/where [[:in :grp (range group-count)]]))
                    (map :val)
                    sort
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

(defn index-test
  [opts]
  (assoc (test opts)
         :client (->CQLSetIndexClient)))
