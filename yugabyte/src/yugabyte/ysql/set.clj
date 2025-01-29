(ns yugabyte.ysql.set
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [yugabyte.ysql.client :as c]))

(def table-name "elements")

;
; Regular set test
;

(defrecord YSQLSetYbClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:val :int "PRIMARY KEY"]])))


  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :add (do (c/insert! c table-name {:val (:value op)})
               (assoc op :type :ok))

      :read (let [value (->> (str "SELECT val FROM " table-name)
                             (c/query c)
                             (mapv :val))]
              (assoc op :type :ok, :value value))))


  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLSetClient YSQLSetYbClient)


;
; Index-governed set test
;

; NOTE: This doesn't work as intended yet as index isn't used for this query
; See https://github.com/YugaByte/yugabyte-db/issues/1554

(def index-name "elements_idx")

(def group-count
  "Number of distinct groups for indexing"
  8)

(def set-index-query
  (str "SELECT val FROM " table-name " WHERE grp " (c/in (range group-count))))

(defrecord YSQLSetIndexYbClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                  [:val :int]
                                                  [:grp :int]]))
    (c/execute! c (str "CREATE INDEX " index-name " ON " table-name " (grp) INCLUDE (val)"))
    (c/assert-involves-index c set-index-query index-name))

  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :add (do (c/insert! op c table-name {:id  (:value op)
                                           :val (:value op)
                                           :grp (rand-int group-count)})
               (assoc op :type :ok))

      :read (let [value (->> set-index-query
                             (c/query op c)
                             (mapv :val))]
              (assoc op :type :ok, :value value))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-index c index-name)
    (c/drop-table c table-name)))


(c/defclient YSQLSetIndexClient YSQLSetIndexYbClient)
