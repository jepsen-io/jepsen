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

(defn read
  "Reads the current value of a key."
  [conn test k]
  (:val (first (j/query conn [(str "select * from test where id = ? "
                                   (:read-lock test))
                              k]))))

(defrecord AtomicClient [conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (j/execute! conn ["create table if not exists test
                      (id int primary key, val int)"]))

  (invoke! [this test op]
    (c/with-error-handling op
      (c/with-txn-aborts op
        (j/with-db-transaction [c conn]
          (let [[id val'] (:value op)]
            (case (:f op)
              :read (assoc op
                           :type  :ok
                           :value (independent/tuple id (read c test id)))

              :write (do (j/execute! c [(str "insert into test (id, val) "
                                             "values (?, ?) "
                                             "on duplicate key update val = ?")
                                        id val' val'])
                         (assoc op :type :ok))

              :cas (let [[expected-val new-val] val'
                         v   (read c test id)]
                     (if (= v expected-val)
                       (do (j/update! c :test {:val new-val} ["id = ?" id])
                           (assoc op :type :ok))
                       (assoc op :type :fail, :error :precondition-failed)))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn test
  [opts]
  (let [n (count (:nodes opts))]
    (basic/basic-test
      (merge
        {:name   "register"
         :client {:client (AtomicClient. nil)
                  :during (independent/concurrent-generator
                            (* 2 n)
                            (range)
                            (fn [k]
                              (->> (gen/reserve n (gen/mix [w cas cas]) r)
                                   (gen/stagger 1/10)
                                   (gen/process-limit 20))))}
         :checker (checker/compose
                    {:perf  (checker/perf)
                     :indep (independent/checker
                              (checker/compose
                                {:timeline (timeline/html)
                                 :linear   (checker/linearizable
                                             {:model (model/cas-register 0)})}))})}
        opts))))
