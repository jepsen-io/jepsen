(ns tidb.adya
  (:refer-clojure :exclude [test])
  (:require
   [jepsen
    [adya :as adya]
    [independent :as independent]
    [client :as client]
    [util :as util :refer [meh letr]]
    [checker :as checker]]
   [tidb.basic :as basic]
   [tidb.sql :refer :all]
   [clojure.java.jdbc :as j]
   [jepsen.client :as client]))

; G2 Anti-dependency cycles
(defrecord G2Client [node]
  client/Client
  (setup! [this test node]
    (j/with-db-connection [c (conn-spec (first (:nodes test)))]
      (with-txn-retries
        (j/execute! c ["drop table if exists a"])
        (j/execute! c ["create table if not exists a
                              (id int primary key, skey int, value int)"]))
      (with-txn-retries
        (j/execute! c ["drop table if exists b"])
        (j/execute! c ["create table if not exists b
                              (id  int primary key, skey int, value int)"])))
    (assoc this :node node))

  (invoke! [this test op]
    ;(with-txn op [c node])
    (let [[k [a-id b-id]] (:value op)]
      (case (:f op)
        :insert
        (with-txn op [c node]
          (letr [order (< (rand) 0.5)
                 as (j/query c [(str "select * from " (if order "a" "b")
                                     " where skey = ? and value % 3 = 0")
                                k])
                 bs (j/query c [(str "select * from " (if order "b" "a")
                                     " where skey = ? and value % 3 = 0")
                                k])
                 = (when (or (seq as) (seq bs))
                     (return (assoc op :type :fail :error :too-late)))
                 table (if a-id "a" "b")
                 id (or a-id b-id)
                 r (j/insert! c table {:skey k, :id id, :value 30})]
                (assoc op :type :ok)))
        :read
        (let [as     (with-txn op [c node]
                       (try
                         (j/query c ["select * from a where
                                    skey = ? and value % 3 = 0" k])))
              bs     (with-txn op [c node]
                       (try
                         (j/query c ["select * from b where
                                    skey = ? and value % 3 = 0" k])))
              values (->> (concat as bs)
                          (map :id))]
          (assoc op
                 :type :ok
                 :value (independent/tuple k values))))))

  (teardown! [this test]))

(defn g2-test
  [opts]
  (basic/basic-test
   (merge {:name    "g2"
           :client  {:client (G2Client. nil)
                     :during (adya/g2-gen)}
           :checker (checker/compose
                     {:perf (checker/perf)
                      :set  (adya/g2-checker)})}
          opts)))
