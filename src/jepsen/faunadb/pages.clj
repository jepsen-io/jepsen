(ns jepsen.faunadb.pages
  "Verifies the transactional isolation of pagination by inserting pairs of
  [-x, x], and concurrently performing reads of every element in the
  collection. We expect to find that for every x, -x exists, and vice versa."
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.set :as set]
            [clojure.core.reducers :as r]
            [dom-top.core :as dt]
            [knossos.op :as op]
            [jepsen [client :as client]
                    [checker :as checker]
                    [fauna :as fauna]
                    [generator :as gen]]
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
    (dt/with-retry [attempts 5]
      (f/query conn
               (q/when (q/not (q/exists? elements))
                 (q/create-class {:name elements-name})))
      (f/query conn
               (q/when (q/not (q/exists? idx))
                 (q/create-index {:name   idx-name
                                  :source elements
                                  :values [{:field ["data" "value"]}]})))
      (catch com.faunadb.client.errors.UnavailableException e
        (if (< 1 attempts)
          (do (info "Waiting for cluster ready")
              (Thread/sleep 1000)
              (retry (dec attempts)))
          (throw e)))))

  (invoke! [this test op]
    (try
      (case (:f op)
        :add (do (f/query conn
                          (q/do*
                            (map (fn [v]
                                   (info "Inserting" v)
                                   (q/create (q/ref elements v)
                                             {:data {:value v}}))
                                 (:value op))))
                 (assoc op :type :ok))

        :read (->> (f/query-all conn (q/match idx)
                                ; TODO: remove this once they've patched the
                                ; iteration bug
                                -10000000)
                   vec
                   (assoc op :type :ok, :value)))
      (catch com.faunadb.client.errors.UnavailableException e
        (assoc op :type :info, :error (.getMessage e)))))

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
    (check [this test model history opts]
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
            ; Now check each read
            errs (->> history
                      (r/filter #(= :read (:f %)))
                      (r/filter op/ok?)
                      (r/map    (fn [op]
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
         :errors errs}))))

(defn test
  [opts]
  (let [zero        0
        n           10000
        group-size  4
        adds (->> (range (- n) n)
                  shuffle
                  (partition group-size)
                  (map (fn [group]
                         {:type  :invoke
                          :f     :add
                          :value group}))
                   (gen/seq))
        reads {:type :invoke, :f :read, :value nil}]
    (fauna/basic-test
      (merge
        {:name "pages"
         :client {:client (PagesClient. nil)
                  :during (->> (gen/mix [adds adds adds adds reads])
                               (gen/stagger 1/5))}
         :checker (checker/compose
                    {:perf  (checker/perf)
                     :pages (checker)})}
        opts))))
