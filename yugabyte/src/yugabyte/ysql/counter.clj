(ns yugabyte.ysql.counter
  "Something like YCQL 'counter' test. SQL does not have counter type though, so we just use int."
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [clojure.java.jdbc :as j]
            [yugabyte.ysql.client :as c]))

(def table-name "counter")

(defrecord YSQLCounterClient [conn setup? teardown?]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/conn-wrapper node)))

  (setup! [this test]
    (c/once-per-cluster
      setup?
      (info "Running setup")
      (c/with-conn
        [c conn]
        (j/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                      [:count :int]]))

        (c/insert! c table-name {:id 0 :count 0}))))


  (invoke! [this test op]
    (c/with-errors
      op
      (c/with-conn
        [c conn]
        (c/with-txn-retry
          (case (:f op)
            ; update! can't handle column references
            :add (do (c/execute! c [(str "UPDATE " table-name " SET count = count + ? WHERE id = 0") (:value op)])
                     (assoc op :type :ok))

            :read (let [value (->> (str "SELECT count FROM " table-name " WHERE id = 0")
                                   (c/query c)
                                   first
                                   (:count))]
                    (assoc op :type :ok :value value)))))))

  (teardown! [this test]
    (c/once-per-cluster
      teardown?
      (info "Running teardown")
      (c/with-timeout
        (c/with-conn
          [c conn]
          (c/with-txn-retry
            (c/drop-table c table-name true))))))

  (close! [this test]
    (rc/close! conn)))
