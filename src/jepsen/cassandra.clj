(ns jepsen.cassandra
  "Cassandra test."
  (:import (com.datastax.driver.core ConsistencyLevel))
  (:require [clojurewerkz.cassaforte.client :as client]
            [clojurewerkz.cassaforte.multi.cql :as cql]
            [jepsen.codec           :as codec])
  (:use
    clojurewerkz.cassaforte.query
    jepsen.util
    jepsen.set-app
    jepsen.load))

(defn cassandra-app
  [opts]
  (let [keyspace "jepsen_keyspace"
        table    "set_app"
        clock-skew (rand-int 100)
        cluster (client/build-cluster {:contact-points [(:host opts)]
                                       :port 9042})
        session (client/connect cluster)]
    (reify SetApp
      (setup [app]
        (teardown app)
        (cql/create-keyspace session keyspace
                             (with {:replication {:class "SimpleStrategy"
                                                  :replication_factor 3}}))
        (cql/use-keyspace session keyspace)
        (cql/create-table session table
                          (column-definitions {:id :int
                                               :elements :blob
                                               :primary-key [:id]}))
        (cql/insert session table
                    {:id 0 :elements (codec/encode [])}
                    (using :timestamp (+ (System/currentTimeMillis)
                                         clock-skew))))

      (add [app element]
        (client/with-consistency-level ConsistencyLevel/QUORUM
          (let [value (-> (cql/select session table (where :id 0))
                          first
                          :elements
                          codec/decode
                          (conj element)
                          codec/encode)]
            (cql/update session table
                        {:elements value}
                        (where :id 0)
                        (using :timestamp (+ (System/currentTimeMillis)
                                             clock-skew)))))
          ok)

      (results [app]
        (client/with-consistency-level ConsistencyLevel/ALL
          (->> (cql/select session table
                           (where :id 0))
               first
               :elements
               codec/decode)))

      (teardown [app]
        (try
          (cql/drop-keyspace session keyspace)
          (catch com.datastax.driver.core.exceptions.InvalidQueryException e
            nil))))))
