(ns jepsen.cockroach.monotonic
  "Monotonic inserts over multiple tables"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as cockroach]
                    [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [reconnect :as rc]
                    [util :as util :refer [meh]]]
            [jepsen.cockroach.client :as c]
            [jepsen.cockroach.nemesis :as cln]
            [jepsen.cockroach.auto :as auto]
            [clojure.core.reducers :as r]
            [clojure.java.jdbc :as j]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op]))

(defn parse-row
  "Translates a DB row back into our local representation, by converting string
  timestamps to bigints."
  [row]
  (update row :sts bigint))

(defn table-name
  "The table name for a given independent key and number."
  [k i]
  (str "k" k "i" i))

(defn monotonic-create-tables!
  "Creates the tables for a given independent key. Takes a connection c, an
  independent-key, and a table count; creates table-count tables for that key.
  If val-as-pkey? is true, assigns :value as the table's primary key."
  [c k table-count val-as-pkey?]
  (locking monotonic-create-tables!
    (doseq [i (range table-count)]
      (let [table (table-name k i)]
        (info "Create table" table)
        (c/with-txn-retry
          (j/execute! c [(str "create table " table
                              " (val int" (if val-as-pkey?  " primary key," ",")
                              "  sts string,"
                              "  node int,"
                              "  process int,"
                              "  tb int)")]))
        (info "Table created" table)))))

(defn q-max
  "On connection `c`, query for the maximum value of `col` in tables `tables`."
  [c col tables]
  (->> tables
       shuffle ; juuust in case deterministic read orders matter
       (map (fn [table]
              ; SQL injections ahoy; we can't use parameters for tables :(
              (-> (c/query c [(str "select max(" (name col) ") as m from "
                                   table)])
                  first
                  :m
                  (or 0))))
       (reduce max)))

(defn insert!
  "Inserts a record and returns that record. Updates the test with that
  record's primary key. If val-as-pkey? is true, uses the :val column.
  Otherwises, uses cockroachdb's implicitly generated row ID."
  [test conn table row]
  (if (:val-as-pkey? test)
    (do (c/insert! conn table row)
        (cockroach/update-keyrange! test table (:val row))
        row)

    (let [row' (c/insert-with-rowid! conn table row)]
      (cockroach/update-keyrange! test table (:rowid row'))
      row')))

; tb is a table identifier--always zero here
; sts is a system timestamp--a 96 bit integer in cockroach, but represented as
; a string in the table and an arbitrary-precision integer in clojure
(defrecord MonotonicClient [independent-keys
                            table-count
                            table-created?
                            ids
                            conn
                            nodenum]
  client/Client

  (open! [this test node]
    (let [n (if (= auto/jdbc-mode :cdb-cluster)
              (.indexOf (vec (:nodes test)) node)
              1)
          conn (c/client node)]
      (info "Setting up client for node " n " - " (name node))
      (assoc this :conn conn :nodenum n)))

  (setup! [this test]
    (locking table-created?
      (when (compare-and-set! table-created? false true)
        (c/with-conn [c conn]
          (doseq [k independent-keys]
            (monotonic-create-tables! c k table-count
                                      (:val-as-pkey? test)))))))

  (invoke! [this test op]
    (let [[k value] (:value op)
          tables    (map table-name (repeat k) (range table-count))]
      (c/with-exception->op op
        (c/with-conn [c conn]
          (case (:f op)
            :add (c/with-txn-retry-as-fail op
                   (c/with-timeout
                     (c/with-txn [c c]
                       (let [;dbtime (c/db-time c)
                             cur-max   (q-max c :val tables)
                             dbtime    (c/db-time c)
                             table-num (rand-int table-count)
                             row {:val      (inc cur-max)
                                  :sts      dbtime
                                  :node     nodenum
                                  :process  (:process op)
                                  :tb       table-num}]
                         (insert! test c (table-name k table-num) row)
                         (assoc op
                                :type :ok
                                :value (independent/tuple k row))))))

            :read
            (c/with-txn-retry
              (c/with-txn [c c]
                (->> tables
                     (mapcat (fn [table]
                               (c/query c [(str "select * from " table
                                                " order by sts")]
                                        {:row-fn parse-row})))
                     (sort-by :sts)
                     vec
                     (independent/tuple k)
                     (assoc op :type :ok, :value)))))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (rc/close! conn)))

(defn non-monotonic
  "Given a comparator (e.g. <, >=), a function f, and an ordered sequence of
  elements xs, returns pairs of successive elements [x x'] where (compare (f x)
  (f x')) does not hold."
  [compare f xs]
  (->> xs
       (partition 2 1)
       (remove (fn [[x x']] (compare (f x) (f x'))))))

(defn non-monotonic-by
  "Like non-monotonic, but breaks up coll by (f coll), and yields a map of (f
  coll) to non-monotonic values."
  [f comparator field coll]
  (->> coll
       (group-by f)
       (map (fn [[k sub-coll]]
              [k (non-monotonic comparator field sub-coll)]))
       (into (sorted-map))))

(defn check-monotonic
  "Checker which verifies that timestamps and values proceed monotonically, as
  well as checking for lost updates, duplicates, etc. If global? is true,
  verifies a global order for values; if global? is false, only checks on a
  per-process basis."
  [linearizable global?]
  (reify checker/Checker
    (check [this test model history opts]
      (let [add-values (->> history
                            (r/filter op/ok?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (into []))
            fail-values (->> history
                             (r/filter op/fail?)
                             (r/filter #(= :add (:f %)))
                             (r/map :value)
                             (into #{}))
            info-values (->> history
                             (r/filter op/info?)
                             (r/filter #(= :add (:f %)))
                             (r/map :value)
                             (into #{}))
            final-read-values (->> history
                                   (r/filter op/ok?)
                                   (r/filter #(= :read (:f %)))
                                   (r/map :value)
                                   (reduce (fn [_ x] x) nil))]
        (if-not final-read-values
          {:valid? :unknown
           :error  "Set was never read"}

          (let [off-order-stss (non-monotonic <= :sts final-read-values)
                off-order-vals (non-monotonic <  :val final-read-values)

                off-order-vals-per-process
                (non-monotonic-by :proc < :val final-read-values)
                off-order-vals-per-node
                (non-monotonic-by :node < :val final-read-values)
                off-order-vals-per-table
                (non-monotonic-by :tb < :val final-read-values)

                fails         (set (map :val fail-values))
                infos         (set (map :val info-values))
                adds          (set (map :val add-values))
                final-reads   (map :val final-read-values)
                dups          (set (for [[id freq] (frequencies final-reads)
                                         :when (> freq 1)]
                                     id))
                final-reads   (set final-reads)

                ;; Lost records are those we definitely added but weren't read
                lost        (set/difference adds final-reads)

                ;; Revived records are those we failed to add but were read
                revived     (set/intersection final-reads fails)

                ;; Recovered records are those we were not sure about and that
                ;; were read
                recovered     (set/intersection final-reads infos)]
            {:valid?          (and (empty? lost)
                                   (empty? dups)
                                   (empty? revived)
                                   (empty? off-order-stss)
                                   (if global?
                                     (empty? off-order-vals)
                                     true)
                                   (every? empty?
                                           (vals off-order-vals-per-process))
                                   ;; linearizability with --linearizable
                                   (or (not linearizable)
                                       (every? empty? off-order-vals)))
             :revived         (util/integer-interval-set-str revived)
             :revived-frac    (util/fraction (count revived) (count fails))
             :recovered       (util/integer-interval-set-str recovered)
             :recovered-frac  (util/fraction (count recovered) (count infos))
             :lost            (util/integer-interval-set-str lost)
             :lost-frac       (util/fraction (count lost) (count adds))
             :duplicates                  dups
             :order-by-errors             off-order-stss
             :value-reorders              off-order-vals
             :value-reorders-per-process  off-order-vals-per-process
             :value-reorders-per-node     off-order-vals-per-node
             :value-reorders-per-table    off-order-vals-per-table}))))))

(defn test
  [opts]
  (let [ks (->> (/ (:concurrency opts)
                   (count (:nodes opts)))
                range)]
    (cockroach/basic-test
      (merge
        {:name        "monotonic"
         :val-as-pkey? false
         :client      {:client (MonotonicClient. ks
                                                 2
                                                 (atom false)
                                                 (atom -1) nil nil)
                       :during (independent/concurrent-generator
                                 (count (:nodes opts))
                                 ks
                                 (fn [k]
                                   (->> {:type :invoke, :f :add, :value nil}
                                        (gen/stagger 0.4))))
                       :final (independent/concurrent-generator
                                (count (:nodes opts))
                                ks
                                (fn [k]
                                  (->> {:type :invoke, :f :read, :value nil}
                                       (gen/stagger 10)
                                       (gen/limit 5))))}
         :checker     (checker/compose
                        {:perf    (checker/perf)
                         :details (independent/checker
                                    (check-monotonic (:linearizable opts)
                                                     true))})}
        opts))))
