(ns yugabyte.ycql.multi-key-acid
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.independent :as independent]
            [jepsen.txn.micro-op :as mop]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.query :as q :refer :all]
            [clojurewerkz.cassaforte.cql :as cql]
            [yugabyte.ycql.client :as c]))

(def table-name "multi_key_acid")
(def keyspace "jepsen")

(c/defclient CQLMultiKey keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table-name
      (q/if-not-exists)
      (q/column-definitions {:id          :int
                             :ik          :int
                             :val         :int
                             :primary-key [:id :ik]})))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (let [[ik txn] (:value op)]
        (case (:f op)
          :read
          (let [ks (map mop/key txn)
                ; Look up values
                vs (->> (cql/select conn table-name
                                    (q/columns :id :val)
                                    (q/where [[= :ik ik]
                                              [:in :id ks]]))
                        (map (juxt :id :val))
                        (into {}))
                ; Rewrite txn to use those values
                txn' (mapv (fn [[f k _]] [f k (get vs k)]) txn)]
            (assoc op :type :ok, :value (independent/tuple ik txn')))

          :write
          ; TODO - temporarily replaced by single DB call until YugaByteDB
          ; supports transaction spanning multiple DB calls.
          ;                 (cassandra/execute conn "BEGIN TRANSACTION")
          ;                 (doseq [[sub-op id val] value]
          ;                   (assert (= sub-op :write))
          ;                   (cql/insert conn table-name {:id id :val val}))
          ;                 (cassandra/execute conn "END TRANSACTION;")
          (do (cassandra/execute conn
                                 (str "BEGIN TRANSACTION "
                                      (->> (for [[f k v] txn]
                                             (do
                                               ; We only support writes
                                               (assert (= :w f))
                                               (str "INSERT INTO "
                                                    keyspace "." table-name
                                                    " (id, ik, val) VALUES ("
                                                    k ", " ik ", " v ");")))
                                           clojure.string/join)
                                      "END TRANSACTION;"))
              (assoc op :type :ok))))))

  (teardown! [this test]))
