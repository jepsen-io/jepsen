(ns yugabyte.ysql.multi-key-acid
  "This test uses INSERT ... ON CONFLICT DO UPDATE"
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.independent :as independent]
            [jepsen.reconnect :as rc]
            [jepsen.txn.micro-op :as mop]
            [yugabyte.ysql.client :as c]))

(def table-name "multi_key_acid")

(defrecord YSQLMultiKeyAcidYbClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:k1 :int]
                                                  [:k2 :int]
                                                  [:val :int]
                                                  ["PRIMARY KEY" "(k1, k2)"]])))

  (invoke-op! [this test op c conn-wrapper]
    (let [[k2 ops] (:value op)]
      (case (:f op)
        :read
        (let [k1s  (map mop/key ops)
              ; Look up values
              vs   (->> (str "SELECT k1, val FROM " table-name " WHERE k2 = " k2 " AND k1 " (c/in k1s))
                        (c/query op c)
                        (map (juxt :k1 :val))
                        (into {}))
              ; Rewrite ops to use those values
              ops' (mapv (fn [[f k1 _]] [f k1 (get vs k1)]) ops)]
          (assoc op :type :ok, :value (independent/tuple k2 ops')))

        :write
        (c/with-txn
          c
          (doseq [[f k1 v] ops]
            (assert (= :w f))
            ; Since there's no UPSERT for SQL...
            (let [update-str (str "INSERT INTO " table-name " (k1, k2, val)"
                                  " VALUES (" k1 ", " k2 ", " v ")"
                                  " ON CONFLICT ON CONSTRAINT " table-name "_pkey"
                                  " DO UPDATE SET val = " v)]
              (c/execute! op c update-str)))
          (assoc op :type :ok)))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLMultiKeyAcidClient YSQLMultiKeyAcidYbClient)
