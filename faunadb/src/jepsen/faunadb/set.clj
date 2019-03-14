(ns jepsen.faunadb.set
  "Set test"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [dom-top.core :as dt]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.faunadb [client :as f]
                            [query :as q]]))

(def side-effects "side-effects")
(def elements "elements")

(def idx-name "all-elements")
(def idx (q/index idx-name))

(defrecord SetClient [opts conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/linearized-client node)))

  (setup! [this test]
    (f/with-retry
      (f/upsert-class! conn {:name elements})
      (f/upsert-class! conn {:name side-effects})
      (f/upsert-index! conn {:name       idx-name
                             :source     (q/class elements)
                             :active     true
                             :serialized (boolean (:serialized-indices test))
                             :values     [{:field ["data" "value"]}]})
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

(defn workload
  [opts]
  {:client (SetClient. opts nil)
   ; Normally you'd want to reserve some threads for reads, but
   ; I've found that with inserts, faunaDB reads grind to a halt,
   ; so we're going to intentionally let reads starve writes so
   ; they have a chance of completing.
   :generator (->> (gen/mix [(adds) (reads)])
                   (gen/stagger 1/5))
   :final-generator (gen/once {:type :invoke, :f :read, :value nil})
   ; We expect SI for regular index queries, serializable for rw queries, and
   ; linearizability with rw serialized indices.
   :checker (checker/set-full
              {:linearizable? (and (:strong-read opts)
                                   (:serialized-indices opts))})})
