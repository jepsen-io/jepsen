(ns yugabyte.ysql.long-fork
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [jepsen.tests.long-fork :as lf]
            [yugabyte.ysql.client :as c]))

(def table-name "long_fork")
(def index-name "long_fork_idx")

(defn long-fork-index-query
  [key-seq]
  (str "SElECT key2, val FROM " table-name " WHERE key2 " (c/in key-seq)))

(defrecord YSQLLongForkYbClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:key :int "PRIMARY KEY"]
                                                  [:key2 :int]
                                                  [:val :int]]))

    (c/execute! c (str "CREATE INDEX " index-name " ON " table-name " (key2) INCLUDE (val)"))
    ; Right now it DOESN'T involve index - but we run it anyway
    ; (c/assert-involves-index c (long-fork-index-query [1 2 3]) index-name)
    )


  (invoke-op! [this test op c conn-wrapper]

    (let [txn (:value op)]
      (case (:f op)
        :read (let [ks   (seq (lf/op-read-keys op))
                    ; Look up values by the value index
                    vs   (->> (long-fork-index-query ks)
                              (c/query op c)
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
                 (do (c/insert! op c table-name {:key  k
                                                 :key2 k
                                                 :val  v})
                     (assoc op :type :ok))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-index c index-name)
    (c/drop-table c table-name)))


(c/defclient YSQLLongForkClient YSQLLongForkYbClient)
