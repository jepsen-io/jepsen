(ns jecci.utils.dbms.sets
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [knossos.op :as op]
            [jecci.utils.dbms.sql :as s]))

(defrecord SetClient [conn translation]
	client/Client
  (open! [this test node]
    (assoc this :conn (s/open node test)))
  (setup! [this test]
    (s/with-conn-failure-retry conn
     (s/execute! conn (:create-table translation))))
  (invoke! [this test op]
    (s/with-error-handling op
     (s/with-txn-aborts op
       (case (:f op)
         :add  (do (s/insert! conn :sets (select-keys op [:value]))
                   (assoc op :type :ok))

         :read (->> (s/query conn (:read-all translation))
                 (mapv :value)
                 (assoc op :type :ok, :value))))))
  (teardown! [this test])
  (close! [this test]
    (s/close! conn)))

(defn gen-SetClient [conn translation]
	(SetClient. conn translation))

(defrecord CasSetClient [conn translation]
	client/Client
  (open! [this test node]
    (assoc this :conn (s/open node test)))
  (setup! [this test]
   (s/with-conn-failure-retry conn
		(s/execute! conn (:create-table translation))))
  (invoke! [this test op]
    (s/with-txn op [c conn]
     (case (:f op)
       :add  (let [e (:value op)]
               (if-let [v (-> (s/query c ((:read-value translation)
                                          (:read-lock test)))
                            first
                            :value)]
                 (s/execute! c ((:update-value translation)
                                v e))
                 (s/insert! c :sets {:id 0, :value (str e)}))
               (assoc op :type :ok))

       :read (let [v (-> (s/query c (:select-0 translation))
                       first
                       :value)
                   v (when v
                       (->> (str/split v #",")
                         (map #(Long/parseLong %))))]
               (assoc op :type :ok, :value v)))))
  (teardown! [this test])
  (close! [this test]
    (s/close! conn)))

(defn gen-CasSetClient [conn translation]
	(CasSetClient. conn translation))
