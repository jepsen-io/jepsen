(ns yugabyte.ycql.set
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info]]
            [clojurewerkz.cassaforte.query :as q]
            [clojurewerkz.cassaforte.cql :as cql]
            [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table "elements")

(c/defclient CQLSetClient keyspace []
             (setup! [this test]
                     (c/create-table conn table
                                     (q/if-not-exists)
                                     (q/column-definitions {:val         :int
                                                            :count       :counter
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
                                                     {:key         :int
                                                      :val         :int
                                                      :grp         :int
                                                      :primary-key [:key]}))
                     (c/create-index conn (str (c/statement->str
                                                 (q/create-index "elements_by_group"
                                                                 (q/on-table table)
                                                                 (q/and-column :grp)))
                                               " INCLUDE (val)")))

             (invoke! [this test op]
                      (c/with-errors op #{:read}
                                     (case (:f op)
                                       :add (do (cql/insert conn table
                                                            {:key (:value op)
                                                             :val (:value op)
                                                             :grp (rand-int group-count)})
                                                (assoc op :type :ok))

                                       :read (->> (cql/select conn table
                                                              (q/columns :val)
                                                              (q/where [[:in :grp (range group-count)]]))
                                                  (map :val)
                                                  sort
                                                  (assoc op :type :ok, :value)))))

             (teardown! [this test]))
