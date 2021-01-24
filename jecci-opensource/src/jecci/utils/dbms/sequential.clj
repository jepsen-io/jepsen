(ns jecci.utils.dbms.sequential
  "A sequential consistency test.

  Verify that client order is consistent with DB order by performing queries
  (in four distinct transactions) like

  A: insert x
  A: insert y
  B: read y
  B: read x

  A's process order enforces that x must be visible before y; we should never
  observe y alone.

  Splits keys up onto different tables to make sure they fall in different
  shard ranges"
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [core :as jepsen]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh]]]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [jecci.utils.dbms.sql :as s]
            [knossos.model :as model]
            [knossos.op :as op]
            [clojure.core.reducers :as r]))


(def table-prefix "String prepended to all table names." "seq_")

(defn table-names
  "Names of all tables"
  [table-count]
  (map (partial str table-prefix) (range table-count)))

(defn key->table
  "Turns a key into a table id"
  [table-count k]
  (str table-prefix (mod (hash k) table-count)))

(defn subkeys
  "The subkeys used for a given key, in order."
  [key-count k]
  (mapv (partial str k "_") (range key-count)))

(defrecord SequentialClient [table-count tbl-created? conn translation]
  client/Client
  (open! [this test node]
    (assoc this :conn (s/open node test)))

  (setup! [this test]
    (locking (type this)
    (s/with-conn-failure-retry conn
      (info "Creating tables" (pr-str (table-names table-count)))
      (doseq [t (table-names table-count)]
        (s/execute! conn ((:create-table translation) t))
        (info "Created table" t)))))
  (invoke! [this test op]
    (let [ks (subkeys (:key-count test) (:value op))]
     (case (:f op)
       :write
       (do (doseq [k ks]
             (let [table (key->table table-count k)]
               (s/with-txn-retries
                 (s/insert! conn table {:tkey k}))))
           (assoc op :type :ok))
       :read
       (->> ks
         reverse
         (mapv (fn [k]
                 (first
                   (s/with-txn-retries
                     (s/query conn ((:read-key translation)
                                    (key->table table-count k) k)
                       {:row-fn :tkey})))))
         (vector (:value op))
         (assoc op :type :ok, :value)))))
  (teardown! [this test])
  (close! [this test]
    (s/close! conn)))

(defn gen-SequentialClient [table-count tbl-created? conn translation]
  (SequentialClient. table-count tbl-created? conn translation))
