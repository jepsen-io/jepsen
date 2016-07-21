(ns jepsen.cockroach.sets
  "Single atomic register test"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as c]
             [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]]
            [clojure.java.jdbc :as :j]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(defn check-sets
  "Given a set of :add operations followed by a final :read, verifies that
  every successfully added element is present in the read, and that the read
  contains only elements for which an add was attempted, and that all
  elements are unique."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [attempts (->> history
                          (r/filter op/invoke?)
                          (r/filter #(= :add (:f %)))
                          (r/map :value)
                          (into #{}))
            adds (->> history
                      (r/filter op/ok?)
                      (r/filter #(= :add (:f %)))
                      (r/map :value)
                      (into #{}))
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
           :error  "Set was never read"})

        (let [final-read  (into #{} final-read-l)

              dups        (into [] (for [[id freq] (frequencies final-read-l) :when (> freq 1)] id))

              ;;The OK set is every read value which we added successfully
              ok          (set/intersection final-read adds)

              ;; Unexpected records are those we *never* attempted.
              unexpected  (set/difference final-read attempts)

              ;; Revived records are those that were reported as failed and still appear.
              revived  (set/intersection final-read fails)

              ;; Lost records are those we definitely added but weren't read
              lost        (set/difference adds final-read)

              ;; Recovered records are those where we didn't know if the add
              ;; succeeded or not, but we found them in the final set.
              recovered   (set/intersection final-read unsure)]

          {:valid?          (and (empty? lost) (empty? unexpected) (empty? dups) (empty? revived))
           :duplicates      dups
           :ok              (util/integer-interval-set-str ok)
           :lost            (util/integer-interval-set-str lost)
           :unexpected      (util/integer-interval-set-str unexpected)
           :recovered       (util/integer-interval-set-str recovered)
           :revived         (util/integer-interval-set-str revived)
           :ok-frac         (util/fraction (count ok) (count attempts))
           :revived-frac    (util/fraction (count revived) (count fails))
           :unexpected-frac (util/fraction (count unexpected) (count attempts))
           :lost-frac       (util/fraction (count lost) (count attempts))
           :recovered-frac  (util/fraction (count recovered) (count attempts))})))))

(defrecord SetsClient [tbl-created?]
  client/Client

  (setup! [this test node]
    (let [conn (init-conn node)]
      (info node "Connected")

      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (Thread/sleep 1000)
          (with-txn-notimeout {} [c conn] (j/execute! c ["drop table if exists set"]))
          (Thread/sleep 1000)
          (info node "Creating table")
          (with-txn-notimeout {} [c conn] (j/execute! c ["create table set (val int)"]))))

      (assoc this :conn conn)))

  (invoke! [this test op]
    (let [conn (:conn this)]
      (case (:f op)
        :add (with-txn op [c conn]
               (do
                 (j/insert! c :set {:val (:value op)})
                 (assoc op :type :ok)))
        :read (with-txn-notimeout op [c conn]
                (->> (j/query c ["select val from set"])
                     (mapv :val)
                     (assoc op :type :ok, :value)))
        )))

  (teardown! [this test]
    (let [conn (:conn this)]
      (meh (with-timeout conn nil
             (j/execute! @conn ["drop table set"])))
      (close-conn @conn)))
  )


(defn sets-test
  [nodes nemesis linearizable]
  (basic-test nodes nemesis linearizable
              {:name        "set"
               :concurrency concurrency-factor
               :client      (SetsClient. (atom false))
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
                               :details  (check-sets)})
               }))
