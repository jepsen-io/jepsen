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

(defrecord SetsClient [opts tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (dt/with-retry [attempts 5]
          (f/query conn (q/create-class {:name elements}))
          (f/query conn (q/create-class {:name side-effects}))
          (f/query
            conn
            (q/create-index
              {:name   idx-name
               :source (q/class elements)
               :values [{:field ["data" "value"]}]}))
          (catch com.faunadb.client.errors.UnavailableException e
            (if (< 1 attempts)
              (do (info "Waiting for cluster ready")
                  (Thread/sleep 1000)
                  (retry (dec attempts)))
              (throw e)))))))

  (invoke! [this test op]
    (try
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
                              (q/create (q/class side-effects) {})
                              r)
                            ; Just a regular read
                            (q/match idx)))
             (into (sorted-set))
             (assoc op :type :ok, :value)))
      (catch java.util.concurrent.ExecutionException e
        (if-let [c (.getCause e)]
          (try
            (throw c)
            (catch com.faunadb.client.errors.UnavailableException e
              (assoc op :type :info, :error (.getMessage e))))
          (throw e)))))

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
       :client {:client (SetsClient. opts (atom false) nil)
                :during (->> (let [a (adds), r (reads)]
                               (gen/mix [a a r]))
                             (gen/stagger 1/5))
                :final (gen/once {:type :invoke, :f :read, :value nil})}
       :checker (checker/compose
                  {:perf     (checker/perf)
                   :set-full (checker/set-full)
                   :set      (checker/set)
                   :timeline (timeline/html)})}
      opts)))
