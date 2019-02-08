(ns yugabyte.long-fork
  "Looks for instances of long fork: a snapshot isolation violation involving
  incompatible orders of writes to disparate objects"
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [clojurewerkz.cassaforte [cql :as cql]
                                     [query :as q]]
            [jepsen [checker    :as checker]
                    [generator  :as gen]]
            [jepsen.tests.long-fork :as lf]
            [yugabyte [client :as c]
                      [core :refer [yugabyte-test]]]))

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
          :read (let [ks (lf/op-read-keys op)
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

(defn index-test
  [opts]
  (let [w (lf/workload 3)]
    (yugabyte-test
      (merge opts
             {:name             "long-fork-index"
              :client           (->CQLLongForkIndexClient)
              :client-generator (->> (:generator w))
              :checker (checker/compose {:perf (checker/perf)
                                         :long-fork (:checker w)})}))))
