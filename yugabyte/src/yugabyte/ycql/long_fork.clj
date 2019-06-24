(ns yugabyte.ycql.long-fork
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.query :as q]
            [jepsen.tests.long-fork :as lf]
            [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table "long_fork")

(c/defclient CQLLongForkIndexClient keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table
      (q/if-not-exists)
      (q/column-definitions {:key   :int
                             :key2  :int
                             :val   :int
                             :primary-key [:key]}))
    (c/create-index conn (str (c/statement->str
                                (q/create-index "long_forks"
                                                (q/on-table table)
                                                (q/and-column :key2)))
                              " INCLUDE (val)")))

  (invoke! [this test op]
    (let [txn (:value op)]
      (c/with-errors op #{}
        (case (:f op)
          :read (let [ks (seq (lf/op-read-keys op))
                      ; Look up values by the value index
                      vs (->> (cql/select conn table
                                          (q/columns :key2 :val)
                                          (q/where [[:in :key2 ks]]))
                              (map (juxt :key2 :val))
                              (into (sorted-map)))
                      ; Rewrite txn to use those values
                      txn' (reduce (fn [txn [f k _]]
                                     ; We already know these are all reads
                                     (conj txn [f k (get vs k)]))
                                   []
                                   txn)]
                  (assoc op :type :ok :value txn'))

          :write (let [[[_ k v]] txn]
                   (do (cql/insert conn table
                                   {:key k
                                    :key2 k
                                    :val v})
                       (assoc op :type :ok)))))))

  (teardown! [this test]))
