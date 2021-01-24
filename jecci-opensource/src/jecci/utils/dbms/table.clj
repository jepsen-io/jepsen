(ns jecci.utils.dbms.table
  "A test for table creation"
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen
             [client :as client]
             [generator :as gen]
             [checker :as checker]]
            [clojure.core.reducers :as r]
            [jecci.utils.dbms.sql :as s]
            [clojure.tools.logging :refer :all]))

(defrecord TableClient [conn last-created-table translation]
  client/Client
  (open! [this test node]
    (assoc this :conn (s/open node test)))
  (setup! [this test])
  (invoke! [this test op]
    (case (:f op)
     :create-table
     (do (s/execute! conn ((:create-table translation) (:value op)))
         (swap! last-created-table (fn [x x'] (if (nil? x)
                                                x'
                                                (max x x')))
           (:value op))
         (assoc op :type :ok))

     :insert
     (try
       (let [[table k] (:value op)]
         (s/insert! conn (str "t" table) {:id k})
         (assoc op :type :ok))
       (catch java.sql.SQLSyntaxErrorException e
         (condp re-find (.getMessage e)
           #"Table .* doesn't exist"
           (assoc op :type :fail, :error :doesn't-exist)

           (throw e)))
       (catch java.sql.SQLIntegrityConstraintViolationException e
         (assoc op :type :fail, :error [:duplicate-key (.getMessage e)])))))
  (teardown! [this test])
  (close! [this test]
    (s/close! conn)))

(defn gen-TableClient [conn last-created-table translation]
  (TableClient. conn last-created-table translation))
