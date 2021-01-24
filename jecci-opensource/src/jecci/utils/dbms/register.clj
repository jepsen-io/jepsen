(ns jecci.utils.dbms.register
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
            [jecci.utils.dbms.sql :as s]
            [knossos.model :as model]))

(defrecord AtomicClient [conn translation]
  client/Client
  (open! [this test node]
    (assoc this :conn (s/open node test)))
  (setup! [this test]
    (s/with-conn-failure-retry conn
     (s/execute! conn (:create-table translation))
     (when (:use-index test)
       (s/create-index! conn (:create-index translation)))))
  (invoke! [thist test op]
    (s/with-error-handling op
     (s/with-txn-aborts op
       (j/with-db-transaction [c conn]
         (let [[id val'] (:value op)]
           (case (:f op)
             :read (assoc op
                     :type  :ok
                     :value (independent/tuple id (:val (first (s/query c
                                                                 ((:read translation)
                                                                  (:use-index test)
                                                                  (:read-lock test)
                                                                  id))))))

             :write (do (s/execute! c ((:insert translation) id val'))
                        (assoc op :type :ok))

             :cas (let [[expected-val new-val] val'
                        v   (:val (first (s/query c
                                           ((:read translation)
                                            (:use-index test)
                                            (:read-lock test)
                                            id))))]
                    (if (= v expected-val)
                      (do (s/update! c :test {:val new-val} ["id = ?" id])
                          (assoc op :type :ok))
                      (assoc op :type :fail, :error :precondition-failed)))))))))
  (teardown! [this test])
  (close! [this test]
    (s/close! conn)))

(defn gen-AtomicClient [conn translation]
  (AtomicClient. conn translation))
