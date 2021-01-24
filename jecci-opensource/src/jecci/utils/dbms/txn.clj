(ns jecci.utils.dbms.txn
  "General transaction interface for database systems"
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
             [generator :as gen]]
            [jecci.utils.dbms.sql :as s :refer :all]
            ))

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn gen-mop!
  [mopTranslation]
  (fn [conn test table-count [f k v]]
    (let [table (table-for table-count k)]
      [f k (case f
             :r (let [ret (-> conn
                        (s/query ((:read mopTranslation)
                                  table
                                  (if (or (:use-index test)
                                        (:predicate-read test))
                                    "sk"
                                    "id")
                                  (:read-lock test) k))
                        first)]
                  (info ret)
                  (info (:val ret))
                  (:val ret))

             :w (do (s/execute! conn ((:insert mopTranslation) table k v))
                    v)

             :append
             (let [r (s/execute!
                       conn
                       ((:insert-cat mopTranslation) table k v))]
               v))])))

(defrecord Client [conn val-type table-count mop! translation]
  client/Client
  (open! [this test node]
    (assoc this :conn (s/open node test)))
  (setup! [this test]
    (dotimes [i table-count]
     (s/with-conn-failure-retry conn
       (s/execute! conn ((:create-table translation)
                         (table-name i) val-type))
       (when (:use-index test)
         (s/create-index! conn ((:create-index translation) (table-name i)))))))
  (invoke! [this test op]
    (let [txn (:value op)
         use-txn? (< 1 (count txn))]
     ;use-txn? false]
     (if use-txn?
       (s/with-txn op [c conn]
         (assoc op :type :ok, :value
                   (mapv (partial mop! c test table-count) txn)))
       (s/with-error-handling op
         (assoc op :type :ok, :value
                   (mapv (partial mop! conn test table-count) txn)))))) 
  (teardown! [this test])
  (close! [this test]
    (s/close! conn)))

(defn gen-Client [conn val-type table-count mop! translation]
  (Client. conn val-type table-count mop! translation))
