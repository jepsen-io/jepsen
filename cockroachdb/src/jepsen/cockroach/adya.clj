(ns jepsen.cockroach.adya
  "Tests for some common anomalies in weaker isolation levels"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as cockroach]
             [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh letr]]
             [reconnect :as rc]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cockroach.client :as c]
            [jepsen.cockroach.nemesis :as cln]
            [clojure.pprint :refer [pprint]]
            [clojure.java.jdbc :as j]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op]))

; G2: Anti-dependency cycles
(defrecord G2Client [table-created? client]
  client/Client
  (setup! [this test node]
    (let [client (c/client node)]
      (locking table-created?
        (when (compare-and-set! table-created? false true)
          (rc/with-conn [c client]
            (c/with-timeout
              (c/with-txn-retry
                (j/execute! c "create table a (
                              id    int primary key,
                              key   int,
                              value int)"))
              (c/with-txn-retry
                (j/execute! c "create table b (
                              id    int primary key,
                              key   int,
                              value int)"))))))
      (assoc this :client client)))

  (invoke! [this test op]
    (c/with-exception->op op
      (rc/with-conn [c client]
        (c/with-timeout
          (let [[k [a-id b-id]] (:value op)]
            (case (:f op)
              :insert
              (c/with-txn [c c]
                (when a-id (cockroach/update-keyrange! test "a" a-id))
                (when b-id (cockroach/update-keyrange! test "b" b-id))
                (letr [as (c/query c ["select * from a where
                                      key = ? and value % 3 = 0" k])
                       bs (c/query c ["select * from b where
                                      key = ? and value % 3 = 0" k])
                       _  (when (or (seq as) (seq bs))
                            ; Ah, the other txn has already committed
                            (return (assoc op :type :fail :error :too-late)))
                       table (if a-id "a" "b")
                       id    (or a-id b-id)
                       r (c/insert! c table {:key k, :id id, :value 30})]
                  (assoc op :type :ok)))

              :read
              (let [as (c/with-txn-retry
                        (c/query c ["select * from a where
                                    key = ? and value % 3 = 0" k]))
                    bs (c/with-txn-retry
                        (c/query c ["select * from b where
                                    key = ? and value % 3 = 0" k]))
                    values (->> (concat as bs)
                                (map :id))]
                    (assoc op
                           :type :ok
                           :value (independent/tuple k values)))))))))

  (teardown! [this test]
    (rc/close! client)))

(defn g2-gen
  "On independent keys, first prepare, then parallel insert."
  []
  (let [ids (atom 0)]
    (independent/concurrent-generator
      2
      (range)
      (fn [k]
        (gen/seq
          [(fn [_  _]
             {:type :invoke :f :insert :value [nil (swap! ids inc)]})
           (fn [_  _]
             {:type :invoke :f :insert :value [(swap! ids inc) nil]})])))))

(defn g2-checker
  []
  (reify checker/Checker
    (check [this test model history opts]
      ; There should be at most one successful insert for any given key
      (let [keys (reduce (fn [m op]
                           (if (= :insert (:f op))
                             (let [k (key (:value op))]
                               (if (= :ok (:type op))
                                 (update m k (fnil inc 0))
                                 (update m k #(or % 0))))
                             m))
                         {}
                         history)
            insert-count (->> keys
                              (filter (fn [[k cnt]] (pos? cnt)))
                              count)
            illegal (->> keys
                         (keep (fn [[k cnt :as pair]]
                                   (when (< 1 cnt) pair)))
                         (into (sorted-set)))]
        {:valid?       (empty? illegal)
         :insert-count insert-count
         :key-count    (count keys)
         :illegal      illegal}))))

(defn g2-test
  [opts]
  (cockroach/basic-test
    (merge
     {:name "g2"
      :client {:client (G2Client. (atom false) nil)
               :during (g2-gen)}
      :checker (checker/compose {:timeline (timeline/html)
                                 :perf     (checker/perf)
                                 :g2       (g2-checker)})}
     opts)))
