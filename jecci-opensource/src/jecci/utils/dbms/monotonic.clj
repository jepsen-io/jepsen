(ns jecci.utils.dbms.monotonic
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]
             [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]
            [jecci.utils.dbms [sql :as s :refer :all]
             [txn :as txn]]))

(defn read-key
  "Read a specific key's value from the table. Missing values are represented
  as -1."
  [c test k]
  (-> (s/query c [(str "select (val) from cycle where "
                    (if (:use-index test) "pk" "sk") " = ?")
                  k])
    first
    (:val -1)))

(defn read-keys
  "Read several keys values from the table, returning a map of keys to values."
  [c test ks]
  (->> (map (partial read-key c test) ks)
    (zipmap ks)
    (into (sorted-map))))

(defrecord IncrementClient [conn translation]
  client/Client
  (open! [this test node]
    (assoc this :conn (s/open node test)))
  (setup! [this test]
    (s/with-conn-failure-retry conn
     (s/execute! conn (:create-table translation))
     (when (:use-index test)
       (s/create-index! conn (:create-index translation)))))
  (invoke! [this test op]
    (s/with-txn op [c conn]
     ;(let [c (:conn this]
     (case (:f op)
       :read (let [v (read-keys c test (shuffle (keys (:value op))))]
               (assoc op :type :ok, :value v))
       :inc (let [k (:value op)]
              (if (:update-in-place test)
                ; Update directly
                (do (when (= [0] (s/execute!
                                   c (:update-cycle translation)))
                      ; That failed; insert
                      (s/insert! c "cycle" {:pk k, :sk k, :val 0}))
                    ; We can't place any constraints on the values since we
                    ; didn't read anything
                    (assoc op :type :ok, :value {}))

                ; Update via separate r/w
                (let [v (read-key c test k)]
                  (if (= -1 v)
                    (s/insert! c "cycle" {:pk k, :sk k, :val 0})
                    (s/update! c "cycle" {:val (inc v)},
                      [(str (if (:use-index test) "sk" "pk") " = ?")
                       k]))
                  ; The monotonic value constraint isn't actually enough to
                  ; capture all the ordering dependencies here: an increment
                  ; from x->y must fall after every read of x, and before
                  ; every read of y, but the monotonic order relation can only
                  ; enforce one of those. We'll return the written value here.
                  ; Still better than nothing.
                  (assoc op :type :ok :value {k (inc v)})))))))
  (teardown! [this test])
  (close! [this test]
    (s/close! conn)))

(defn gen-IncrementClient [conn translation]
  (IncrementClient. conn translation))
