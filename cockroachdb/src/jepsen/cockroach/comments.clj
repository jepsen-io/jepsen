(ns jepsen.cockroach.comments
  "Checks for a strict serializability anomaly in which T1 < T2, but T2 is
  visible without T1.

  We perform concurrent blind inserts across n tables, and meanwhile, perform
  reads of both tables in a transaction. To verify, we replay the history,
  tracking the writes which were known to have completed before the invocation
  of any write w_i. If w_i is visible, and some w_j < w_i is *not* visible,
  we've found a violation of strict serializability.

  Splits keys up onto different tables to make sure they fall in different
  shard ranges"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as cockroach]
                    [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [util :as util :refer [meh]]
                    [reconnect :as rc]]
            [jepsen.cockroach.client :as c]
            [jepsen.cockroach.nemesis :as cln]
            [clojure.java.jdbc :as j]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op]))

(def table-prefix "String prepended to all table names." "comment_")

(defn table-names
  "Names of all tables"
  [table-count]
  (map (partial str table-prefix) (range table-count)))

(defn id->table
  "Turns an id into a table id"
  [table-count id]
  (str table-prefix (mod (hash id) table-count)))

(defrecord Client [table-count table-created? client]
  client/Client

  (setup! [this test node]
    (Thread/sleep 2000)
    (let [client (c/client node)]
      (locking table-created?
        (when (compare-and-set! table-created? false true)
          (c/with-conn [c client]
            (c/with-timeout
              (info "Creating tables" (pr-str (table-names table-count)))
              (doseq [t (table-names table-count)]
                (j/execute! c [(str "create table " t
                                    " (id int primary key,
                                       key int)")])
                (info "Created table" t))))))

      (assoc this :client client)))

  (invoke! [this test op]
    (c/with-exception->op op
      (c/with-conn [c client]
        (c/with-timeout
            (case (:f op)
              :write (let [[k id] (:value op)
                           table (id->table table-count id)]
                       (c/insert! c table {:id id, :key k})
                       (cockroach/update-keyrange! test table id)
                       (assoc op :type :ok))

              :read (c/with-txn [c c]
                      (->> (table-names table-count)
                           (mapcat (fn [table]
                                     (c/query c [(str "select id from "
                                                      table
                                                      " where key = ?")
                                                 (key (:value op))])))
                           (map :id)
                           (into (sorted-set))
                           (independent/tuple (key (:value op)))
                           (assoc op :type :ok, :value))))))))

  (teardown! [this test]
    (rc/close! client)))

(defn checker
  []
  (reify checker/Checker
    (check [this test model history opts]
      ; Determine first-order write precedence graph
      (let [expected (loop [completed  (sorted-set)
                            expected   {}
                            [op & more :as history] history]
                       (cond
                         ; Done
                         (not (seq history))
                         expected

                         ; We know this value is definitely written
                         (= :write (:f op))
                         (cond ; Write is beginning; record precedence
                               (op/invoke? op)
                               (recur completed
                                      (assoc expected (:value op) completed)
                                      more)

                               ; Write is completing; we can now expect to see
                               ; it
                               (op/ok? op)
                               (recur (conj completed (:value op))
                                      expected more)

                               true
                               (recur completed expected more))

                         true
                         (recur completed expected more)))
            errors (->> history
                        (r/filter op/ok?)
                        (r/filter #(= :read (:f %)))
                        (reduce (fn [errors op]
                                  (let [seen         (:value op)
                                        our-expected (->> seen
                                                          (map expected)
                                                          (reduce set/union))
                                        missing (set/difference our-expected
                                                                seen)]
                                    (if (empty? missing)
                                      errors
                                      (conj errors
                                            (-> op
                                                (dissoc :value)
                                                (assoc :missing missing)
                                                (assoc :expected-count
                                                       (count our-expected)))))))
                                []))]
        {:valid? (empty? errors)
         :errors errors}))))

(defn reads [] {:type :invoke, :f :read, :value nil})
(defn writes []
  (->> (range)
       (map (fn [k] {:type :invoke, :f :write, :value k}))
       gen/seq))

(defn test
  [opts]
  (let [reads (reads)
        writes (writes)]
    (cockroach/basic-test
      (merge
        {:name   "comments"
         :client {:client (Client. 10 (atom false) nil)
                  :during (independent/concurrent-generator
                            (count (:nodes opts))
                            (range)
                            (fn [k]
                              (->> (gen/mix [reads writes])
                                   (gen/stagger 1/100)
                                   (gen/limit 500))))
                  :final  nil}
         :checker (checker/compose
                    {:perf       (checker/perf)
                     :sequential (independent/checker (checker))})}
        opts))))
