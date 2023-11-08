(ns yugabyte.ysql.append
  "Values are lists of integers. Each operation performs a transaction,
  comprised of micro-operations which are either reads of some value (returning
  the entire list) or appends (adding a single number to whatever the present
  value of the given list is). We detect cycles in these transactions using
  Jepsen's cycle-detection system."
  (:require [clojure.string :as str]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [clojure.data.json :as json]
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

(def keys-per-row 2)

(defn row-for
  "What row should we use for the given key?"
  [test k]
  (quot k keys-per-row))

(defn col-for
  "What column should we use for the given key?"
  [test k]
  (str "v" (mod k keys-per-row)))

(defn select-with-optional-lock
  [locking col table]
  (let [clause (if (= :pessimistic locking)
                 (rand-nth ["" " for update" " for no key update" " for share" " for key share"])
                 "")]
    (str "select (" col ") from " table " where k = ?" clause)))

(defn get-geo-insert-column
  [geo-partitioning]
  (if (= geo-partitioning :geo)
    (str ", geo_partition")
    ""))

(defn insert-primary-geo
  [conn table geo-partitioning col row v geo-row]
  (c/execute! conn
              [(str "insert into " table
                    " (k, k2, " col (get-geo-insert-column geo-partitioning) ")"
                    " values (?, ?, ?, ?)") row row v geo-row]))

(defn insert-primary
  [conn table col row v]
  (c/execute! conn
              [(str "insert into " table
                    " (k, k2, " col ")"
                    " values (?, ?, ?)") row row v]))

(defn geo-row-update
  [geo-partitioning v]
  (if (= geo-partitioning :geo)
    (str ", geo_partition = '" (mod v 2) "a'")
    ""))

(defn read-primary
  "Reads a key based on primary key"
  [locking conn table row col]
  (some-> conn
          (c/query [(select-with-optional-lock locking col table) row])
          first
          (get (keyword col))
          (str/split #",")
          (->>                                              ; Append might generate a leading , if the row already exists
            (remove str/blank?)
            (mapv #(Long/parseLong %)))))

(defn append-primary!
  "Writes a key based on primary key."
  [locking geo-partitioning conn table row col v]
  (let [_ (if (= :pessimistic locking)
            (do
              ; Randomly evaluate SELECT FOR UPDATE with timeout in case of pessimistic locking
              (c/query conn [(select-with-optional-lock locking col table) row])
              (Thread/sleep (rand-int 2000)))
            nil)
        r (c/execute! conn [(str "update " table
                                 " set " col " = CONCAT(" col ", ',', ?)" (geo-row-update geo-partitioning v) " "
                                 "where k = ?") v row])]
    (when (= [0] r)
      ; No rows updated
      (if (= geo-partitioning :geo)
        (insert-primary-geo conn table geo-partitioning col row v (str (mod v 2) "a"))
        (insert-primary conn table col row v))) v))

(defn read-secondary
  "Reads a key based on a predicate over a secondary key, k2"
  [conn table row col]
  (some-> conn
          (c/query [(str "select (" col ") from " table
                         " where (k2 * 2) - ? = ?")
                    col col])
          first
          (get (keyword col))
          (str/split #",")
          (->> (mapv #(Long/parseLong %)))))

(defn append-secondary!
  "Writes a key based on a predicate over a secondary key, k2. Returns v."
  [conn table row col v]
  (let [r (c/execute! conn [(str "update " table
                                 " set " col " = CONCAT(" col ", ',', ?) "
                                 "where (k2 * 2) - ? = ?") v row row])]
    (when (= [0] r)
      ; No rows updated
      (c/execute! conn
                  [(str "insert into " table
                        "(k, k2, " col ") values (?, ?, ?)") row row v]))
    v))

(defn mop!
  "Executes a transactional micro-op of the form [f k v] on a connection, where
  f is either :r for read or :append for list append. Returns the completed
  micro-op."
  [geo-partitioning locking conn test [f k v]]
  (let [table (table-for test k)
        row (row-for test k)
        col (col-for test k)]
    [f k (case f
           :r
           (read-primary locking conn table row col)

           :append
           (append-primary! locking geo-partitioning conn table row col v))]))

(defn create-geo-tablespace
  [conn tablespace-name replica-placement]
  (info "Creating tablespace" tablespace-name)
  (j/execute! conn
              [(str "CREATE TABLESPACE " tablespace-name " "
                    "WITH (replica_placement='" (json/write-str replica-placement) "');")]
              {:transaction? false}))

(defn setup-geo-partition
  [conn geo-partitioning tablespace-name]
  (if (= geo-partitioning :geo)
    (do
      (create-geo-tablespace
        conn
        (str tablespace-name "_1a")
        {
         :num_replicas     2
         :placement_blocks [
                            {
                             :cloud             :ybc
                             :region            :jepsen-1
                             :zone              :jepsen-1a
                             :min_num_replicas  1
                             :leader_preference 1
                             }
                            ]
         })
      (create-geo-tablespace
        conn
        (str tablespace-name "_2a")
        {
         :num_replicas     2
         :placement_blocks [
                            {
                             :cloud             :ybc
                             :region            :jepsen-2
                             :zone              :jepsen-2a
                             :min_num_replicas  1
                             :leader_preference 1
                             }
                            ]
         }))))

(defn get-create-table-columns-clause
  [geo-partitioning]
  (if (= geo-partitioning :geo)
    [[:k :int]
     [:k2 :int]
     [:geo_partition :varchar]]
    [;[:k :int "unique"]
     [:k :int "PRIMARY KEY"]
     [:k2 :int]]))

(defn get-table-spec
  [geo-partitioning]
  (if (= geo-partitioning :geo)
    "PARTITION BY LIST (geo_partition)"
    ""))

(defn create-partitioning-table
  [c table tablespace-name postfix]
  (info (str "Create table partitions for " table "_" postfix " for '" postfix "'"))
  (c/execute! c (str "CREATE TABLE " table "_" postfix " "
                     "PARTITION OF " table " (k, k2, geo_partition"
                     ", PRIMARY KEY (k, geo_partition)) FOR VALUES IN ('" postfix "') "
                     "TABLESPACE " tablespace-name "_" postfix)))

(defrecord InternalClient [isolation locking geo-partitioning]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (let [tablespace-name "geo_tablespace"]
      (info "Create tablespace " tablespace-name)
      (setup-geo-partition c geo-partitioning tablespace-name)
      (->> (range (table-count test))
           (map table-name)
           (map (fn [table]
                  (info "Creating table" table)
                  (c/execute! c (j/create-table-ddl
                                  table
                                  (into
                                    (get-create-table-columns-clause geo-partitioning)
                                    ; Columns for n values packed in this row
                                    (map (fn [i] [(col-for test i) :text])
                                         (range keys-per-row)))
                                  {:conditional? true
                                   :table-spec   (get-table-spec geo-partitioning)}))
                  (if (= geo-partitioning :geo)
                    (do
                      (create-partitioning-table c table tablespace-name "1a")
                      (create-partitioning-table c table tablespace-name "2a")))
                  ))
           dorun)))

  (invoke-op! [this test op c conn-wrapper]
    (let [txn (:value op)
          use-txn? (< 1 (count txn))
          txn' (if use-txn?
                 (j/with-db-transaction [c c {:isolation isolation}]
                                        (mapv (partial mop! geo-partitioning locking c test) txn))
                 (mapv (partial mop! geo-partitioning locking c test) txn))]
      (assoc op :type :ok, :value txn'))))

(c/defclient Client InternalClient)
