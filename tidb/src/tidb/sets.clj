(ns tidb.sets
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [knossos.op :as op]
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]))

(defrecord SetClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (c/execute! conn ["create table if not exists sets
                        (id     int not null primary key auto_increment,
                        value  bigint not null)"])))

  (invoke! [this test op]
    (c/with-error-handling op
      (c/with-txn-aborts op
        (case (:f op)
          :add  (do (c/insert! conn :sets (select-keys op [:value]))
                    (assoc op :type :ok))

          :read (->> (c/query conn ["select * from sets"])
                     (mapv :value)
                     (assoc op :type :ok, :value))))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

; This variant does compare-and-set on a single text value to reveal lost
; updates.
(defrecord CasSetClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (c/with-conn-failure-retry conn
      (c/execute! conn ["create table if not exists sets
                        (id     int not null primary key,
                        value   text)"])))

  (invoke! [this test op]
    (c/with-txn op [c conn]
      (case (:f op)
        :add  (let [e (:value op)]
                (if-let [v (-> (c/query c [(str "select (value) from sets"
                                                   " where id = 0 "
                                                   (:read-lock test))])
                               first
                               :value)]
                  (c/execute! c ["update sets set value = ? where id = 0"
                                    (str v "," e)])
                  (c/insert! c :sets {:id 0, :value (str e)}))
                (assoc op :type :ok))

        :read (let [v (-> (c/query c ["select (value) from sets where id = 0"])
                          first
                          :value)
                    v (when v
                        (->> (str/split v #",")
                             (map #(Long/parseLong %))))]
                (assoc op :type :ok, :value v)))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       (gen/seq)))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn workload
  [opts]
  (let [c (:concurrency opts)]
    {:client (SetClient. nil)
     :generator (->> (gen/reserve (/ c 2) (adds) (reads))
                     (gen/stagger 1/10))
     :checker (checker/set-full)}))

(defn cas-workload
  [opts]
  (assoc (workload opts) :client (CasSetClient. nil)))
