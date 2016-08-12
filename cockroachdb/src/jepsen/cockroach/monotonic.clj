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
            [clojure.core.reducers :as r]
            [clojure.java.jdbc :as j]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op]))

;; number of tables involved in the monotonic-multitable tests
(def multitable-spread 2)

(defn non-monotonic
  "Given a comparator (e.g. <, >=), a function f, and an ordered sequence of
  elements xs, returns pairs of successive elements [x x'] where (compare (f x)
  (f x')) does not hold."
  [compare f xs]
  (->> xs
       (partition 2 1)
       (remove (fn [[x x']] (compare (f x) (f x'))))))

(defn check-monotonic
  "Given a set of :add operations followed by a final :read, verifies that
  every potentially added element is present in the read, and that the read
  contains only elements for which an add was succesful, and that all
  elements are unique and in the same order as database timestamps."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [; all-adds-l and final-read-l are vectors of op :values
            all-adds-l (->> history
                            (r/filter op/ok?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (into []))
            final-read-l (->> history
                              (r/filter op/ok?)
                              (r/filter #(= :read (:f %)))
                              (r/map :value)
                              (reduce (fn [_ x] x) nil))

            ; Fails and unsure are sets of vals
            fails (->> history
                            (r/filter op/fail?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (r/map :val)
                            (into #{}))
            unsure (->> history
                            (r/filter op/info?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (r/map :val)
                            (into #{}))]
        (if-not final-read-l
          {:valid? false
           :error  "Set was never read"}
          (let [; TODO: why don't we verify timestamps are totally ordered?
                off-order-sts (non-monotonic <= :sts final-read-l)
                off-order-val (non-monotonic <  :val final-read-l)

                adds   (set (map :val all-adds-l))
                lok    (mapv :val final-read-l)
                ok     (set lok)
                dups   (set (for [[id freq] (frequencies lok)
                                  :when (> freq 1)]
                              id))

                ;; Lost records are those we definitely added but weren't read
                lost        (set/difference adds ok)

                ; TODO: ok and fails have different representations and will
                ; never intersect

                ;; Revived records are those we failed to add but were read
                revived     (set/intersection ok fails)

                ;; Recovered records are those we were not sure about and that
                ;; were read
                recovered     (set/intersection ok unsure)]
            {:valid?          (and (empty? lost)
                                   (empty? dups)
                                   (empty? off-order-sts)
                                   (empty? off-order-val)
                                   (empty? revived))
             :revived         (util/integer-interval-set-str revived)
             :revived-frac    (util/fraction (count revived) (count fails))
             :recovered       (util/integer-interval-set-str recovered)
             :recovered-frac  (util/fraction (count recovered) (count unsure))
             :lost            (util/integer-interval-set-str lost)
             :lost-frac       (util/fraction (count lost) (count adds))
             :duplicates      dups
             :order-by-errors off-order-sts
             :value-reorders  off-order-val}))))))

(defn parse-row
  "Translates a DB row back into our local representation, by converting string
  timestamps to bigints."
  [row]
  (update row :sts bigint))

; tb is a table identifier--always zero here
; sts is a system timestamp--a 96 bit integer in cockroach, but represented as
; a string in the table and an arbitrary-precision integer in clojure

; Rows are externally represented as (val, sts, node, tb)
(defrecord MonotonicClient [tbl-created? conn nodenum]
  client/Client

  (setup! [this test node]
    (let [n (if (= c/jdbc-mode :cdb-cluster)
              (.indexOf (vec (:nodes test)) node)
              1)
          conn (c/client node)]
      (info "Setting up client for node " n " - " (name node))

      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (rc/with-conn [c conn]
            (c/with-timeout
              (j/execute! c ["drop table if exists mono"])
              (info node "Creating table")
              (j/execute!
                c ["create table mono (val int, sts string, node int, tb int)"])
              (j/insert! c :mono {:val -1 :sts "0" :node -1 :tb -1})))))

      (assoc this :conn conn :nodenum n)))

  (invoke! [this test op]
    (c/with-exception->op op
      (rc/with-conn [c conn]
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
                  (j/insert! c :mono {:val   (inc curmax)
                                      :sts   dbtime
                                      :node  nodenum
                                      :tb    0})
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

(defn test
  [opts]
  (c/basic-test
    (merge
      {:name        "monotonic"
       :concurrency c/concurrency-factor
       :client      {:client (MonotonicClient. (atom false) nil nil)
                     :during (->> (range)
                                  (map (partial array-map
                                                :type :invoke
                                                :f :add
                                                :value))
                                  gen/seq
                                  (gen/stagger 1))
                     :final (->> {:type :invoke, :f :read, :value nil}
                                 gen/once
                                 gen/clients)}
       :checker     (checker/compose
                      {:perf    (checker/perf)
                       :details (check-monotonic)})}
      opts)))


(defn check-monotonic-split
  "Same as check-monotonic, but check ordering only from each client's
  perspective."
  [linearizable numnodes]
  (reify checker/Checker
    (check [this test model history opts]
      (let [all-adds-l (->> history
                            (r/filter op/ok?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (into []))
            fails (->> history
                      (r/filter op/fail?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
            unsure (->> history
                        (r/filter op/info?)
                        (r/filter #(= :add (:f %)))
                        (r/map :value)
                        (into #{}))
            final-read-l (->> history
                              (r/filter op/ok?)
                              (r/filter #(= :read (:f %)))
                              (r/map :value)
                              (reduce (fn [_ x] x) nil))]
        (if-not final-read-l
          {:valid? false
           :error  "Set was never read"}

          (let [off-order-sts (non-monotonic <= :sts final-read-l)
                off-order-val (non-monotonic <  :val final-read-l)

                ; TODO: ??????
                filter (fn [col]
                         (fn [x]
                           (->> final-read-l
                                (r/filter #(= x (get % col)))
                                (into []))))

                test-off-order (fn [col src]
                                 (->> src
                                      (map (filter col))
                                      (map (partial non-monotonic < :val))))
                processes (set (map :process final-read-l))
                off-order-val-perclient (test-off-order :proc processes)
                off-order-val-pernode   (test-off-order :node (range numnodes))
                off-order-val-pertable  (test-off-order
                                          :tb (range multitable-spread))

                ; TODO: standardize on :value or (-> :value :val), the
                ; difference/intersections below don't use the same
                ; representation
                adds    (set (map :value all-adds-l))
                lok     (mapv first final-read-l)
                ok      (set lok)

                dups        (set (for [[id freq] (frequencies lok)
                                       :when (> freq 1)]
                                   id))

                ;; Lost records are those we definitely added but weren't read
                lost        (set/difference adds ok)

                ;; Revived records are those we failed to add but were read
                revived     (set/intersection ok fails)

                ;; Recovered records are those we were not sure about and that
                ;; were read
                recovered     (set/intersection ok unsure)]
            {:valid?          (and (empty? lost)
                                   (empty? dups)
                                   (empty? revived)
                                   (every? empty? off-order-sts)
                                   ;; serializability per client
                                   (every? empty? off-order-val-perclient)

                                   ;; linearizability with --linearizable
                                   (or (not linearizable)
                                       (every? empty? off-order-val)))
             :revived         (util/integer-interval-set-str revived)
             :revived-frac    (util/fraction (count revived) (count fails))
             :recovered       (util/integer-interval-set-str recovered)
             :recovered-frac  (util/fraction (count recovered) (count unsure))
             :lost            (util/integer-interval-set-str lost)
             :lost-frac       (util/fraction (count lost) (count adds))
             :duplicates                dups
             :order-by-errors           off-order-sts
             :value-reorders            off-order-val
             :value-reorders-perclient  off-order-val-perclient
             :value-reorders-pernode    off-order-val-pernode
             :value-reorders-pertable   off-order-val-pertable}))))))

; Tuples are [val sts process table node]
(defrecord MultitableClient [tbl-created? cnt conn nodenum]
  client/Client
  (setup! [this test node]
    (let [n (if (= c/jdbc-mode :cdb-cluster)
              (.indexOf (vec (:nodes test)) node)
              1)
          conn (c/client node)]

      (info "Setting up client " n " for " (name node))

      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (rc/with-conn [c conn]
            (doseq [x (range multitable-spread)]
              (do
                (j/execute! c [(str "drop table if exists mono" x)])
                (info "Creating table " x)
                (j/execute! c [(str "create table mono" x
                                    " (val int, sts string, node int,"
                                    " proc int, tb int)")])
                (j/insert! c (str "mono" x)
                           {:val -1
                            :sts "0"
                            :node -1
                            :proc -1
                            :tb x}))))))

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

(defn multitable-test
  [opts]
  (c/basic-test
    (merge
      {:name        "monotonic-multitable"
       :concurrency c/concurrency-factor
       :client      {:client (MultitableClient. (atom false) (atom 0) nil nil)
                     :during (->> (range)
                                  (map (partial array-map
                                                :type :invoke
                                                :f :add
                                                :value))
                                  gen/seq
                                  (gen/stagger 1))
                     :final (gen/clients
                              (gen/once {:type :invoke, :f :read, :value nil}))}
       :checker     (checker/compose
                      {:perf     (checker/perf)
                       :details  (check-monotonic-split
                                   (:linearizable opts)
                                   (count (:nodes test)))})}
      opts)))
