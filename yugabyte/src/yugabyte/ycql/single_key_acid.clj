(ns yugabyte.ycql.single-key-acid
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.independent :as independent]
            [knossos.model :as model]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clojurewerkz.cassaforte.query :refer :all]
            [clojurewerkz.cassaforte.policies :refer :all]
            [clojurewerkz.cassaforte.cql :as cql]
            [yugabyte.ycql.client :as c]))

(def keyspace "jepsen")
(def table-name "single_key_acid")

(c/defclient CQLSingleKey keyspace []
  (setup! [this test]
    (c/create-table conn table-name
                    (if-not-exists)
                    (column-definitions {:id :int
                                         :val :int
                                         :primary-key [:id]})))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (let [[id val] (:value op)]
        (case (:f op)
          :write
          (do (cql/insert-with-ks conn keyspace table-name
                                  {:id id, :val val})
              (assoc op :type :ok))

          :cas
          (let [[expected-val new-val] val
                res (cql/update-with-ks conn keyspace table-name
                                        {:val new-val}
                                        (only-if [[= :val expected-val]])
                                        (where [[= :id id]]))
                applied (get (first res) (keyword "[applied]"))]
            (assoc op :type (if applied :ok :fail)))

          :read
          (let [value (->> (cql/select-with-ks conn keyspace table-name
                                               (where [[= :id id]]))
                           first
                           :val)]
            (assoc op :type :ok :value (independent/tuple id value)))))))

  (teardown! [this test]))
