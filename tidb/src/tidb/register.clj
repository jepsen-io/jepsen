(ns tidb.register
  "Single atomic register test"
  (:refer-clojure :exclude [test read])
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.linearizable-register :as lr]
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
  (:val (first (c/query conn [(str "select (val) from test where "
                                   (if (:use-index test) "sk" "id") " = ? "
                                   (:read-lock test))
                              k]))))

(defrecord AtomicClient [conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (c/execute! conn ["create table if not exists test
                        (id   int primary key,
                         sk   int,
                         val  int)"])
      (when (:use-index test)
        (c/create-index! conn ["create index test_sk_val on test (sk, val)"]))))

  (invoke! [this test op]
    (c/with-error-handling op
      (c/with-txn-aborts op
        (j/with-db-transaction [c conn]
          (let [[id val'] (:value op)]
            (case (:f op)
              :read (assoc op
                           :type  :ok
                           :value (independent/tuple id (read c test id)))

              :write (do (c/execute! c [(str "insert into test (id, sk, val) "
                                             "values (?, ?, ?) "
                                             "on duplicate key update "
                                             "val = ?")
                                        id id val' val'])
                         (assoc op :type :ok))

              :cas (let [[expected-val new-val] val'
                         v   (read c test id)]
                     (if (= v expected-val)
                       (do (c/update! c :test {:val new-val} ["id = ?" id])
                           (assoc op :type :ok))
                       (assoc op :type :fail, :error :precondition-failed)))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  [opts]
  (let [w (lr/test (assoc opts :model (model/cas-register 0)))]
    (-> w
        (assoc :client (AtomicClient. nil))
        (update :generator (partial gen/stagger 1/10)))))
