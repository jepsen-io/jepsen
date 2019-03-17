(ns jepsen.faunadb.pages
  "Verifies the transactional isolation of pagination by inserting groups of
  elements together like [1, 5, -15, 23], then concurrently performing reads of
  every element in the collection. We expect to find that for every element of
  a group, all the other elements exist."
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.set :as set]
            [clojure.core.reducers :as r]
            [dom-top.core :as dt]
            [knossos.op :as op]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [util :as util]]
            [jepsen.faunadb [query :as q]
                            [client :as f]]))

(def elements-name "elements")
(def elements (q/class elements-name))

(def idx-name "all-elements")
(def idx (q/index idx-name))

(defrecord PagesClient [conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (f/with-retry
      (f/upsert-class! conn {:name elements-name})
      (f/upsert-index! conn {:name        idx-name
                             :source      elements
                             :active      true
                             :serialized  (boolean
                                            (:serialized-indices test))
                             ; :partitions 1
                             :terms  [{:field ["data" "key"]}]
                             :values [{:field ["data" "value"]}]})
      (f/wait-for-index conn idx)))

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (f/with-errors op #{:read}
        (case (:f op)
          :add (do (f/query conn
                            (q/do*
                              (map (fn [v]
                                     (q/create elements
                                               {:data {:key k
                                                       :value v}}))
                                   v)))
                   (assoc op :type :ok))

          :read (->> (f/query-all conn (q/match idx k))
                     vec
                     (independent/tuple k)
                     (assoc op :type :ok, :value))))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn read-errs
  "Given an index of elements to the groups of elements that were added
  together, and a set of elements, returns a set of errors for any cases where
  this set can't be expressed as the union of some set of adds."
  ([idx read]
   (let [errs (read-errs idx read #{})]
     (when (seq errs)
       errs)))
  ([idx read errs]
   ; Trivial case: an empty read is safe
   (if (empty? read)
     errs
     ; Recursive case: pick an element and cross off everything added with it
     (let [e     (first read)
           add   (get idx e)
           ok?   (every? read add)
           read' (set/difference read add)
           ;_     (info :read-errs e :add add :ok? ok?)
           ;_     (info :read- read)
           ;_     (info :read' read')
           err   (when-not ok?
                   {:expected add
                    :found    (set/intersection read add)})]
       (recur idx read' (if err (conj errs err) errs))))))

(defn checker
  "Examines all :add transactions, constructing a set of elements which should
  appear together, or not at all. Then checks each read to make sure it is
  expressible as the union of some set of adds."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [adds    (filter #(= :add (:f %)) history)
            invokes (->> adds
                         (r/filter op/invoke?)
                         (r/map :value)
                         (into #{}))
            fails   (->> adds
                         (r/filter op/fail?)
                         (r/map :value)
                         (into #{}))
            ; OK, now let's take the adds that could possibly have succeeded
            adds    (set/difference invokes fails)
            ; Now compute an index of individual elements to all elements added
            ; in that group
            idx (persistent!
                  (reduce (fn [idx xs]
                            (let [xs (set xs)]
                              (reduce (fn [idx x]
                                        (assert (not (contains? idx x))
                                                "Elements must be unique")
                                        (assoc! idx x xs))
                                      idx
                                      xs)))
                          (transient {})
                          adds))
            ok-reads (->> history
                          (r/filter #(= :read (:f %)))
                          (r/filter op/ok?)
                          (into []))
            ; Now check each read
            errs (->> ok-reads
                      (r/map (fn [op]
                               (let [v  (:value op)
                                     v' (set v)]
                                 (if-not (= (count v) (count v'))
                                   {:op     op
                                    :errors [:duplicate-items]}
                                   (when-let [errs (read-errs idx v')]
                                     {:op     op
                                      :errors errs})))))
                      (r/filter identity)
                      (into []))]
        {:valid? (not (seq errs))
         :ok-read-count (count ok-reads)
         :error-count   (count errs)
         :first-error   (first errs)
         :worst-error   (util/max-by (comp count :errors) errs)}))))

(defn workload
  [opts]
  (let [zero        0
        n           10000
        group-size  4]
    {:client    (PagesClient. nil)
     :generator (->> (independent/concurrent-generator
                       (* 2 (count (:nodes opts)))
                       (range)
                       (fn [k]
                         (let [adds (->> (range (- n) n)
                                         shuffle
                                         (partition group-size)
                                         (map (fn [group]
                                                {:type  :invoke
                                                 :f     :add
                                                 :value group}))
                                         (gen/seq))
                               reads {:type :invoke, :f :read, :value nil}]
                           (->> (gen/mix [adds adds adds adds reads])
                                (gen/limit 256)
                                (gen/stagger 1/5))))))
     :checker   (independent/checker (checker))}))
