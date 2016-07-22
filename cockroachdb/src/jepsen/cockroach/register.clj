(ns jepsen.cockroach.register
  "Single atomic register test"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as c]
                    [cockroach-nemesis :as cln]
                    [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [util :refer [meh]]]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord AtomicClient [tbl-created?]
  client/Client

  (setup! [this test node]
    (let [conn (c/init-conn node)]
      (info node "Connected")
      ;; Everyone's gotta block until we've made the table.
      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (Thread/sleep 1000)
          (c/with-txn-notimeout {} [c conn] (j/execute! c ["drop table if exists test"]))
          (Thread/sleep 1000)
          (info node "Creating table")
          (c/with-txn-notimeout {} [c conn] (j/execute! c ["create table test (id int, val int)"]))))

      (assoc this :conn conn)))

  (invoke! [this test op]
    (let [conn (:conn this)]
      (c/with-txn op [c conn]
        (let [id     (key (:value op))
              value  (val (:value op))
              val'    (->> (j/query c ["select val from test where id = ?" id] :row-fn :val)
                           (first))]
          (case (:f op)
            :read (assoc op :type :ok, :value (independent/tuple id val'))

            :write (do
                     (if (nil? val')
                       (j/insert! c :test {:id id :val value})
                       (j/update! c :test {:val value} ["id = ?" id]))
                     (assoc op :type :ok))

            :cas (let [[value' value] value
                       cnt (j/update! c :test {:val value} ["id = ? and val = ?" id value'])]
                   (assoc op :type (if (zero? (first cnt)) :fail :ok))))
          ))))

  (teardown! [this test]
    (let [conn (:conn this)]
      (meh (c/with-timeout conn nil
             (j/execute! @conn ["drop table test"])))
      (c/close-conn @conn))
    ))

(defn test
  [opts]
  (c/basic-test
    (merge
      {:name    "atomic"
       :concurrency c/concurrency-factor
       :client  (AtomicClient. (atom false))
       :generator (->> (independent/sequential-generator
                         (range)
                         (fn [k]
                           (->> (gen/reserve 5 (gen/mix [w cas]) r)
                                (gen/delay 0.5)
                                (gen/limit 60))))
                       (gen/stagger 1)
                       (cln/with-nemesis (:generator (:nemesis opts))))

       :model   (model/cas-register 0)
       :checker (checker/compose
                  {:perf   (checker/perf)
                   :details (independent/checker checker/linearizable) })}
      opts)))
