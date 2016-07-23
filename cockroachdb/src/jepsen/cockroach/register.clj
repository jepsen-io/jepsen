(ns jepsen.cockroach.register
  "Single atomic register test"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as c]
                    [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cockroach.nemesis :as cln]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord AtomicClient [tbl-created? conn]
  client/Client

  (setup! [this test node]
    (let [conn (c/init-conn node)]
      (info node "Connected")
      ;; Everyone's gotta block until we've made the table.
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (Thread/sleep 1000)
          (c/with-txn-notimeout {} [c conn]
            (j/execute! c ["drop table if exists test"]))
          (Thread/sleep 1000)
          (info node "Creating table")
          (c/with-txn-notimeout {} [c conn]
            (j/execute! c ["create table test (id int, val int)"]))))

      (assoc this :conn conn)))

  (invoke! [this test op]
    (c/with-idempotent #{:read}
      (c/with-txn op [c conn]
        (let [id     (key (:value op))
              val'   (val (:value op))
              val    (first (j/query c ["select val from test where id = ?" id]
                                     :row-fn :val))]
          (case (:f op)
            :read (assoc op :type :ok, :value (independent/tuple id val))

            :write (do
                     (if (nil? val)
                       (j/insert! c :test {:id id :val val'})
                       (j/update! c :test {:val val'} ["id = ?" id]))
                     (assoc op :type :ok))

            :cas (let [[expected-val new-val] val'
                       cnt (j/update! c :test {:val new-val}
                                      ["id = ? and val = ?" id expected-val])]
                   (assoc op :type (if (zero? (first cnt)) :fail :ok))))))))

  (teardown! [this test]
    (meh (c/with-timeout conn nil
           (j/execute! @conn ["drop table test"])))
    (c/close-conn @conn)))

(defn test
  [opts]
  (c/basic-test
    (merge
      {:name        "atomic"
       :concurrency c/concurrency-factor
       :client      (AtomicClient. (atom false) nil)
       :generator   (->> (independent/concurrent-generator 10
                           (range)
                           (fn [k]
                             (->> (gen/reserve 5 (gen/mix [w cas]) r)
                                  (gen/delay 0.5)
                                  (gen/limit 60))))
                         (gen/stagger 1)
                         (cln/with-nemesis (:generator (:nemesis opts))))

       :model       (model/cas-register 0)
       :checker     (checker/compose
                      {:perf   (checker/perf)
                       :details (independent/checker
                                  (checker/compose
                                    {:timeline     (timeline/html)
                                     :linearizable checker/linearizable}))})}
      opts)))
