(ns jepsen.faunadb.sets
  "Set test"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [dom-top.core :as dt]
            [jepsen [client :as client]
                    [checker :as checker]
                    [fauna :as fauna]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.faunadb [client :as f]
                            [query :as q]]))

(def side-effects "side-effects")
(def elements "elements")

(def idx-name "all-elements")
(def idx (q/index idx-name))

(defrecord SetsClient [opts conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (f/with-retry
      (f/query conn
               (q/do (f/upsert-class {:name elements})
                     (f/upsert-class {:name side-effects})))
      (f/query conn
               (f/upsert-index
                 {:name       idx-name
                  :source     (q/class elements)
                  :serialized (boolean (:serialized-indices test))
                  :values     [{:field ["data" "value"]}]}))
      (f/wait-for-index conn idx)))

  (invoke! [this test op]
    (f/with-errors op #{:read}
      (case (:f op)
        :add
        (let [v (:value op)]
          (f/query
            conn
            (q/create
              (q/ref elements v)
              {:data {:value v}}))
          (assoc op :type :ok))

        :read
        (->> (f/query-all conn
                          (if (:strong-read opts)
                            ; We're gonna do our read, then sneak a write in
                            ; to force this txn to be strict serializable
                            (q/let [r (q/match idx)]
                              (q/at (q/time "now")
                                (q/create (q/class side-effects) {}))
                              r)
                            ; Just a regular read
                            (q/match idx))
                          {:size 1024})
             (into (sorted-set))
             (assoc op :type :ok, :value)))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn adds
  "Set adds"
  []
  (->> (range)
       (map (partial array-map
                     :type :invoke
                     :f :add
                     :value))
       gen/seq))

(defn reads
  "Set reads"
  []
  {:type :invoke, :f :read, :value nil})

(defn test
  [opts]
  (fauna/basic-test
    (merge
      {:name   "set"
       :client {:client (SetsClient. opts nil)
                :during (->> (gen/reserve (/ (:concurrency opts) 2) (adds)
                                          (reads))
                             (gen/stagger 1/5))
                :final (gen/once {:type :invoke, :f :read, :value nil})}
       :checker (checker/compose
                  {:perf     (checker/perf)
                   :set-full (checker/set-full {:linearizable? true})
                   ;:timeline (timeline/html)
                   :set      (checker/set)})}
      opts)))
