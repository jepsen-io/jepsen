(ns jepsen.cockroach.monotonic
  "Monotonic inserts over multiple tables"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as c]
                    [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]]
            [clojure.java.jdbc :as :j]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(defn check-order
  "Detect monotonicity errors over a result set (internal function)."
  [iprev irows ires1 ires2]
  (loop [prev iprev
         rows irows
         res1 ires1
         res2 ires2]
    (if (empty? rows) [res1 res2]
        (let [row (first rows)
              nres1 (concat res1 (if (> (nth prev 1) (nth row 1)) (list (list prev row)) ()))
              nres2 (concat res2 (if (>= (first prev) (first row)) (list (list prev row)) ()))]
          (recur row (rest rows) nres1 nres2)))))

(defn monotonic-order
  "Detect monotonicity errors over a result set."
  [rows]
  (check-order (first rows) (rest rows) () ()))

(defn check-monotonic
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was succesful, and that all
  elements are unique and in the same order as database timestamps."
  [linearizable]
  (reify checker/Checker
    (check [this test model history opts]
      (let [all-adds-l (->> history
                            (r/filter op/ok?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (into []))
            final-read-l (->> history
                              (r/filter op/ok?)
                              (r/filter #(= :read (:f %)))
                              (r/map :value)
                              (reduce (fn [_ x] x) nil)
                              )
            fails (->> history
                            (r/filter op/fail?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (into #{}))
                                          
            unsure (->> history
                            (r/filter op/info?)
                            (r/filter #(= :add (:f %)))
                            (r/map :value)
                            (into #{}))]
        (if-not final-read-l
          {:valid? false
           :error  "Set was never read"})
        (let [
              [off-order-sts off-order-val] (monotonic-order final-read-l)

              adds   (into #{} (map first all-adds-l))
              lok     (into [] (map first final-read-l))
              ok      (into #{} lok)
              
              dups        (into #{} (for [[id freq] (frequencies lok) :when (> freq 1)] id))

              ;; Lost records are those we definitely added but weren't read
              lost        (set/difference adds ok)

              ;; Revived records are those we failed to add but were read
              revived     (set/intersection ok fails)

              ;; Recovered records are those we were not sure about and that were read
              recovered     (set/intersection ok unsure)
              ]
          {:valid?          (and (empty? lost) (empty? dups) (empty? off-order-sts)
                                 (empty? off-order-val) (empty? revived)
                                 )
           :revived         (util/integer-interval-set-str revived)
           :revived-frac    (util/fraction (count revived) (count fails))        
           :recovered       (util/integer-interval-set-str recovered)
           :recovered-frac  (util/fraction (count recovered) (count unsure))
           :lost            (util/integer-interval-set-str lost)
           :lost-frac       (util/fraction (count lost) (count adds))
           :duplicates      dups
           :order-by-errors off-order-sts
           :value-reorders  off-order-val
           }
          )))))


(defrecord MonotonicClient [tbl-created?]
  client/Client

  (setup! [this test node]
    (let [n (if (= jdbc-mode :cdb-cluster) (str->int (subs (name node) 1 2)) 1)
          conn (init-conn node)]
      (info "Setting up client for node " n " - " (name node))

      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (Thread/sleep 1000)
          (with-txn-notimeout {} [c conn] (j/execute! c ["drop table if exists mono"]))
          (Thread/sleep 1000)
          (info node "Creating table")
          (with-txn-notimeout {} [c conn] (j/execute! c ["create table mono (val int, sts string, node int, tb int)"]))
          (Thread/sleep 1000)
          (with-txn-notimeout {} [c conn] (j/insert! c :mono {:val -1 :sts "0" :node -1 :tb -1}))))

      (assoc this :conn conn :nodenum n)))

  (invoke! [this test op]
    (let [conn (:conn this)
          nodenum (:nodenum this)]
        (case (:f op)
          :add (with-txn op [c conn]
                 (let [curmax (->> (j/query c ["select max(val) as m from mono"] :row-fn :m)
                                   (first))
                       currow (->> (j/query c ["select * from mono where val = ?" curmax])
                                   (map (fn [row] (list (:val row) (str->int (:sts row)) (:node row) (:tb row))))
                                   (first))
                       dbtime (db-time c)]
                   (j/insert! c :mono {:val (+ 1 curmax) :sts dbtime :node nodenum :tb 0})
                   (assoc op :type :ok, :value currow)))
          
          :read (with-txn-notimeout op [c conn]
                  (->> (j/query c ["select * from mono order by sts"])
                     (map (fn [row] (list (:val row) (str->int (:sts row)) (:node row) (:tb row))))
                     (into [])
                     (assoc op :type :ok, :value)))
          )))

  (teardown! [this test]
    (let [conn (:conn this)]
      (meh (with-timeout conn nil
             (j/execute! @conn ["drop table if exists mono"])))
      (close-conn @conn)))
  )


(defn monotonic-test
  [nodes nemesis linearizable]
  (basic-test nodes nemesis linearizable
              {:name        "monotonic"
               :concurrency concurrency-factor
               :client      (MonotonicClient. (atom false))
               :generator   (gen/phases
                             (->> (range)
                                  (map (partial array-map
                                                :type :invoke
                                                :f :add
                                                :value))
                                  gen/seq
                                  (gen/stagger 1)
                                  (cln/with-nemesis (:generator nemesis)))
                             (->> {:type :invoke, :f :read, :value nil}
                                  gen/once
                                  gen/clients))
               :checker     (checker/compose
                             {:perf    (checker/perf)
                              :details (check-monotonic linearizable)})
               }))


(defn check-monotonic-split
  "Same as check-monotonic, but check ordering only from each client's perspective."
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
                              (reduce (fn [_ x] x) nil)
                              )]
        (if-not final-read-l
          {:valid? false
           :error  "Set was never read"})

        (let [[off-order-sts off-order-val] (monotonic-order final-read-l)

              filter (fn [col] (fn [x] (->> final-read-l
                                            (r/filter #(= x (nth % col)))
                                            (into ())
                                            (reverse))))

              test-off-order (fn [col src] (->> src
                                                (map (filter col))
                                                (map monotonic-order)
                                                (map last)))
              processes (->> final-read-l (map #(nth % 2)) (into #{}))
              off-order-val-perclient (test-off-order 2 processes)
              off-order-val-pernode (test-off-order 4 (map #(+ 1 %) (range numnodes)))
              off-order-val-pertable (test-off-order 3 (range multitable-spread))

              adds (into #{} (map first all-adds-l))
              lok     (into [] (map first final-read-l))
              ok      (into #{} lok)

              dups        (into #{} (for [[id freq] (frequencies lok) :when (> freq 1)] id))

              ;; Lost records are those we definitely added but weren't read
              lost        (set/difference adds ok)

              ;; Revived records are those we failed to add but were read
              revived     (set/intersection ok fails)

              ;; Recovered records are those we were not sure about and that were read
              recovered     (set/intersection ok unsure)
              ]
{:valid?          (and (empty? lost)
                                     (empty? dups)
                                     (empty? revived)
                                     (every? true? (into [] (map empty? off-order-sts)))
                                     ;; serializability per client
                                     (every? true? (into [] (map empty? off-order-val-perclient)))

                                     ;; linearizability with --linearizable
                                     (or (not linearizable) (every? true? (into [] (map empty? off-order-val))))
                                     )
               :revived         (util/integer-interval-set-str revived)
               :revived-frac    (util/fraction (count revived) (count fails))        
               :recovered       (util/integer-interval-set-str recovered)
               :recovered-frac  (util/fraction (count recovered) (count unsure))
               :lost            (util/integer-interval-set-str lost)
               :lost-frac       (util/fraction (count lost) (count adds))
               :duplicates      dups
               :order-by-errors off-order-sts
               :value-reorders  off-order-val
               :value-reorders-perclient off-order-val-perclient
               :value-reorders-pernode off-order-val-pernode
               :value-reorders-pertable off-order-val-pertable
               })))))

(defrecord MultitableClient [tbl-created? cnt]
  client/Client
  (setup! [this test node]
    (let [n (if (= jdbc-mode :cdb-cluster) (str->int (subs (name node) 1 2)) 1)
          conn (init-conn node)]

      (info "Setting up client " n " for " (name node))

      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (dorun (for [x (range multitable-spread)]
                   (do
                     (Thread/sleep 1000)
                     (with-txn-notimeout {} [c conn] (j/execute! c [(str "drop table if exists mono" x)]))
                     (Thread/sleep 1000)
                     (info "Creating table " x)
                     (with-txn-notimeout {} [c conn] (j/execute! c [(str "create table mono" x
                                             " (val int, sts string, node int, proc int, tb int)")]))
                     (Thread/sleep 1000)
                     (with-txn-notimeout {} [c conn] (j/insert! c (str "mono" x) {:val -1 :sts "0" :node -1 :proc -1 :tb x}))
                     )))))

      (assoc this :conn conn :nodenum n)))


  (invoke! [this test op]
    (let [conn (:conn this)
          nodenum (:nodenum this)]
      (case (:f op)
        :add  (locking cnt
                (with-txn op [c conn]
                  (let [rt (rand-int multitable-spread)
                        dbtime (db-time c)
                        v (swap! cnt inc)]
                    (j/insert! c (str "mono" rt) {:val v :sts dbtime :node nodenum :proc (:process op) :tb rt})
                    (assoc op :type :ok, :value [v dbtime (:process op) rt nodenum]))))
                
        :read (with-txn-notimeout op [c conn]
                (->> (range multitable-spread)
                     (map (fn [x]
                            (->> (j/query c [(str "select * from mono" x " where node <> -1")])
                                 (map (fn [row] (list (:val row) (str->int (:sts row)) (:proc row) (:tb row) (:node row))))
                                 )))
                     (reduce concat)
                     (sort-by (fn [x] (nth x 1)))
                     (into [])
                     (assoc op :type :ok, :value)))
        )))

  (teardown! [this test]
    (let [conn (:conn this)]
      (dorun (for [x (range multitable-spread)]
               (with-timeout conn nil
                 (j/execute! @conn [(str "drop table if exists mono" x)]))))
      (close-conn @conn)))
  )

(defn monotonic-multitable-test
  [nodes nemesis linearizable]
  (basic-test nodes nemesis linearizable
              {:name        "monotonic-multitable"
               :concurrency concurrency-factor
               :client      (MultitableClient. (atom false) (atom 0))
               :generator   (gen/phases
                             (->> (range)
                                  (map (partial array-map
                                                :type :invoke
                                                :f :add
                                                :value))
                                  gen/seq
                                  (gen/stagger 1)
                                  (cln/with-nemesis (:generator nemesis)))
                             (->> {:type :invoke, :f :read, :value nil}
                                  gen/once
                                  gen/clients))
               :checker     (checker/compose
                             {:perf     (checker/perf)
                              :details  (check-monotonic-split linearizable (count nodes))})
               }))
