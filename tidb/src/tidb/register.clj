(ns tidb.register
  "Single atomic register test"
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]
            [knossos.model :as model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord AtomicClient [conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (j/execute! conn ["drop table if exists test"])
    (j/execute! conn ["create table if not exists test
                      (id int primary key, val int)"]))

  (invoke! [this test op]
    (with-txn op [c conn]
      (try
        (let [id   (key (:value op))
              val' (val (:value op))
              val  (-> c (j/query [(str "select * from test where id = ? FOR UPDATE") id] :row-fn :val) first)]
          (case (:f op)
            :read (assoc op :type :ok, :value (independent/tuple id val))

            :write (do
                     (if (nil? val)
                       (j/insert! c :test {:id id :val val'})
                       (j/update! c :test {:val val'} ["id = ?" id]))
                     (assoc op :type :ok))

            :cas (let [[expected-val new-val] val'
                       cnt (j/update! c :test {:val new-val} ["id = ? and val = ?" id expected-val])]
                   (assoc op :type (if (zero? (first cnt))
                                     :fail
                                     :ok))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn test
  [opts]
  (basic/basic-test
    (merge
      {:name        "register"
       :client      {:client (AtomicClient. nil)
                     :during (independent/concurrent-generator
                               10
                               (range)
                               (fn [k]
                                 (->> (gen/reserve 5 (gen/mix [w cas cas]) r)
                                      (gen/delay-til 1/2)
                                      (gen/stagger 0.1)
                                      (gen/limit 100))))}
       :checker     (checker/compose
                      {:perf   (checker/perf)
                       :indep (independent/checker
                                (checker/compose
                                  {:timeline (timeline/html)
                                   :linear   (checker/linearizable
                                               {:model (model/cas-register 0)})}))})}
      opts)))
