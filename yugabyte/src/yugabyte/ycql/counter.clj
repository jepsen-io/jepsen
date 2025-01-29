(ns yugabyte.ycql.counter
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.query :refer :all]
            [clojurewerkz.cassaforte.policies :refer :all]
            [clojurewerkz.cassaforte.cql :as cql]
            [yugabyte.ycql.client :as c]))

(def table-name "counter")
(def keyspace "jepsen")

(c/defclient CQLCounterClient keyspace []
             (setup! [this test]
                     (c/create-table conn table-name
                                     (if-not-exists)
                                     (column-definitions {:id          :int
                                                          :count       :counter
                                                          :primary-key [:id]}))
                     (cql/update conn table-name {:count (increment-by 0)}
                                 (where [[= :id 0]])))

             (invoke! [this test op]
                      (c/with-errors op #{:read}
                                     (case (:f op)
                                       :add (do (cql/update-with-ks conn keyspace table-name
                                                                    {:count (increment-by (:value op))}
                                                                    (where [[= :id 0]]))
                                                (assoc op :type :ok))

                                       :read (let [value (->> (cql/select-with-ks conn keyspace table-name
                                                                                  (where [[= :id 0]]))
                                                              first
                                                              :count)]
                                               (assoc op :type :ok :value value)))))

             (teardown! [this test]))
