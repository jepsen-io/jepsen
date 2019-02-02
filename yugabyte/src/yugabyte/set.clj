(ns yugabyte.set
  "Adds elements to sets and reads them back"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]
                    [checker :as checker]]
            [clojurewerkz.cassaforte [client :as cass]
                                     [query :as q]
                                     [cql :as cql]]
            [yugabyte [client :as c]
                      [core :refer [yugabyte-test]]]))

(def setup-lock (Object.))
(def keyspace "jepsen")
(def table "elements")

(c/defclient CQLSetClient []
  (setup! [this test]
    (locking setup-lock
      (c/ensure-keyspace! conn keyspace test)
      (cql/use-keyspace conn keyspace)
      (c/create-table conn table
                      (q/if-not-exists)
                      (q/column-definitions {:value :int
                                             :primary-key [:value]}))))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :add (do (cql/insert-with-ks conn keyspace table
                                     {:value (:value op)})
                 (assoc op :type :ok))

        :read (->> (cql/select-with-ks conn keyspace table)
                   (map :value)
                   ; sort
                   (into (sorted-set))
                   (assoc op :type :ok, :value)))))

  (teardown! [this test]))

(defn adds
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn reads
  []
  {:type :invoke, :f :read, :value nil})

(defn test
  [opts]
  (yugabyte-test
    (merge opts
           {:name "set"
            :client (CQLSetClient. nil)
            :client-generator (->> ;(gen/reserve (/ (:concurrency opts) 2) (adds)
                                   ; (reads))
                                   (gen/mix [(adds) (reads)])
                                   (gen/stagger 1/10))
            :checker (checker/compose {:perf (checker/perf)
                                       :set (checker/set-full)})})))
