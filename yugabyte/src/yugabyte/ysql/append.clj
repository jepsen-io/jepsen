(ns yugabyte.ysql.append
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util]]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as append]
            [yugabyte.ysql.client :as c]))

(defn table-count
  "How many tables do we need for a test?"
  [test]
  (:table-count test 5))

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "append" i))

(defn table-for
  "What table should we use for the given key?"
  [test k]
  (table-name (mod (hash k) (table-count test))))

(defn mop!
  "Executes a transactional micro-op of the form [f k v] on a connection, where
  f is either :r for read or :append for list append. Returns the completed
  micro-op."
  [conn test [f k v]]
  (let [table (table-for test k)]
    (info :table table)
    [f k (case f
           :r (some-> conn
                      (c/query [(str "select (v) from " table
                                     " where k = ?") k])
                      first
                      :v
                      (str/split #",")
                      (->> (mapv #(Long/parseLong %))))

           :append
           (do (c/execute! conn
                           ; on conflict, in postgres, needs fully qualified
                           ; column names.
                           [(str "insert into " table " (k, v) values (?, ?) "
                                 ; I think on conflict here breaks *some* of
                                 ; the time, but not always, when we use a
                                 ; unique constraint instead of primary key.
                                 "on conflict (k) do update set "
                                 ; Nope, it breaks both ways!
                                 ; "on conflict on constraint append_k_key do update set "
                                 "v = CONCAT(" table ".v, ',', ?)")
                            k (str v) (str v)])
               v))]))


(defrecord InternalClient []
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (->> (range (table-count test))
         (map table-name)
         (map (fn [table]
                (info "Creating table" table)
                (c/execute! c (j/create-table-ddl table
                                                  [[:k :int "unique"]
                                                   [:v :text]]))))
                                                 ;["PRIMARY KEY" "(k)"]]))))
         dorun))

  (invoke-op! [this test op c conn-wrapper]
    (let [txn       (:value op)
          use-txn?  (< 1 (count txn))
          ; use-txn?  false ; Just for making sure the checker actually works
          txn'      (if use-txn?
                      (c/with-txn c
                        (mapv (partial mop! c test) txn))
                      (mapv (partial mop! c test) txn))]
      (assoc op :type :ok, :value txn'))))

(c/defclient Client InternalClient)
