(ns jepsen.cockroach.monotonic
  "Monotonic inserts over multiple tables"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as c]
                    [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [reconnect :as rc]
                    [util :as util :refer [meh]]]
            [jepsen.cockroach.nemesis :as cln]
            [jepsen.cockroach.auto :as auto]
            [clojure.core.reducers :as r]
            [clojure.java.jdbc :as j]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op]))

;; number of tables involved in the monotonic-multitable tests
(def multitable-spread 2)

(defn parse-row
  "Translates a DB row back into our local representation, by converting string
  timestamps to bigints."
  [row]
  (update row :sts bigint))

; tb is a table identifier--always zero here
; sts is a system timestamp--a 96 bit integer in cockroach, but represented as
; a string in the table and an arbitrary-precision integer in clojure
(defrecord MonotonicClient [tbl-created? conn nodenum]
  client/Client

  (setup! [this test node]
    (let [n (if (= auto/jdbc-mode :cdb-cluster)
              (.indexOf (vec (:nodes test)) node)
              1)
          conn (c/client node)]
      (info "Setting up client for node " n " - " (name node))

      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (rc/with-conn [c conn]
            (c/with-timeout
              (c/with-txn-retry
                (j/execute! c ["drop table if exists mono"]))
              (info node "Creating table")
              (c/with-txn-retry
                (j/execute!
                  c ["create table mono (val int, sts string, node int, process int, tb int)"]))
              (c/with-txn-retry
                (j/insert! c :mono {:val -1 :sts "0" :node -1, :process -1, :tb -1}))))))

      (assoc this :conn conn :nodenum n)))

  (invoke! [this test op]
    (c/with-exception->op op
      (rc/with-conn [c conn]
        (assert c)
        (case (:f op)
          :add
          (c/with-timeout
            (c/with-txn-retry
              (c/with-txn [c c]
                (let [curmax (->> (j/query c
                                           ["select max(val) as m from mono"]
                                           :row-fn :m)
                                  (first))
                      currow (->> (j/query c ["select * from mono where val = ?"
                                              curmax]
                                           :row-fn parse-row)
                                  (first))
                      dbtime (c/db-time c)]
                  (j/insert! c :mono {:val      (inc curmax)
                                      :sts      dbtime
                                      :node     nodenum
                                      :process  (:process op)
                                      :tb       0})
                  ; TODO: return new row, not previous row
                  (assoc op :type :ok, :value currow)))))

          :read
          (c/with-txn-retry
            (c/with-txn [c c]
              (->> (j/query c ["select * from mono order by sts"]
                            :row-fn parse-row)
                   vec
                   (assoc op :type :ok, :value))))))))

  (teardown! [this test]
    (try
      (c/with-timeout
        (rc/with-conn [c conn]
          (j/execute! c ["drop table if exists mono"])))
      (finally
        (rc/close! conn)))))

(defrecord MultitableClient [tbl-created? cnt conn nodenum]
  client/Client
  (setup! [this test node]
    (let [n (if (= auto/jdbc-mode :cdb-cluster)
              (.indexOf (vec (:nodes test)) node)
              1)
          conn (c/client node)]

      (info "Setting up client " n " for " (name node))

      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (rc/with-conn [c conn]
            (doseq [x (range multitable-spread)]
              (do
                (c/with-txn-retry
                  (j/execute! c [(str "drop table if exists mono" x)]))
                (info "Creating table " x)
                (c/with-txn-retry
                  (j/execute! c [(str "create table mono" x
                                      " (val int, sts string, node int,"
                                      " proc int, tb int)")]))
                (c/with-txn-retry
                  (j/insert! c (str "mono" x)
                             {:val -1
                              :sts "0"
                              :node -1
                              :proc -1
                              :tb x})))))))

      (assoc this :conn conn :nodenum n)))

  (invoke! [this test op]
    (c/with-exception->op op
      (rc/with-conn [c conn]
        (case (:f op)
          ; This removes all concurrency from the test, which may not be what
          ; we want.
          :add (locking cnt
                 (c/with-timeout
                   (c/with-txn-retry
                     (let [rt      (rand-int multitable-spread)
                           dbtime  (c/db-time c)
                           v       (swap! cnt inc)
                           row     {:val v
                                    :sts dbtime
                                    :proc (:process op)
                                    :tb   rt
                                    :node nodenum}]
                       (j/insert! c (str "mono" rt) row)
                       (assoc op :type :ok :value row)))))

          :read (c/with-txn-retry
                  (c/with-txn [c c]
                    (->> (range multitable-spread)
                         (mapcat (fn [x]
                                   (j/query c [(str "select * from mono" x
                                                    " where node <> -1")]
                                            :row-fn parse-row)))
                         (sort-by :sts)
                         vec
                         (assoc op :type :ok, :value))))))))

  (teardown! [this test]
    (try
      (c/with-timeout
        (rc/with-conn [c conn]
          (doseq [x (range multitable-spread)]
            (j/execute! c [(str "drop table if exists mono" x)]))))
      (finally
        (rc/close! conn)))))


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
          {:valid? false
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
  (c/basic-test
    (merge
      {:name        "monotonic"
       :client      {:client (MonotonicClient. (atom false) nil nil)
                     :during (->> {:type :invoke, :f :add, :value nil}
                                  (gen/stagger 1))
                     :final (->> {:type :invoke, :f :read, :value nil}
                                 (gen/stagger 10)
                                 (gen/limit 5))}
       :checker     (checker/compose
                      {:perf    (checker/perf)
                       :details (check-monotonic (:linearizable opts)
                                                 true)})}
      opts)))

(defn multitable-test
  [opts]
  (c/basic-test
    (merge
      {:name        "monotonic-multitable"
       :client      {:client (MultitableClient. (atom false) (atom 0) nil nil)
                     :during (->> {:type :invoke, :f :add, :value nil}
                                  (gen/stagger 1))
                     :final (->> {:type :invoke, :f :read, :value nil}
                                 (gen/stagger 10)
                                 (gen/limit 5))}
       :checker     (checker/compose
                      {:perf     (checker/perf)
                       :details  (check-monotonic
                                   (:linearizable opts)
                                   false)})}
      opts)))
