(ns yugabyte.counter
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client    :as client]
             [checker   :as checker]
             [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [clojurewerkz.cassaforte [client :as cassandra]
             [query :refer :all]
             [policies :refer :all]
             [cql :as cql]]
            [yugabyte [auto :as auto]
                      [client :as c]
                      [core :refer :all]]))

(def setup-lock (Object.))
(def keyspace "jepsen")
(def table-name "counters")

(c/defclient CQLCounterClient []
  (setup! [this test]
    (locking setup-lock
      (cql/create-keyspace conn keyspace
                           (if-not-exists)
                           (with {:replication
                                  {"class"              "SimpleStrategy"
                                   "replication_factor" 3}}))
      (cql/use-keyspace conn keyspace)
      (cql/create-table conn table-name
                        (if-not-exists)
                        (column-definitions {:id :int
                                             :count :counter
                                             :primary-key [:id]}))
      (cql/update conn table-name {:count (increment-by 0)}
                  (where [[= :id 0]]))))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (case (:f op)
        :add (do (cql/update-with-ks conn keyspace table-name
                                     {:count (increment-by (:value op))}
                                     (where [[= :id 0]]))
                 (assoc op :type :ok))

        :read (let [value (->> (cql/select-with-ks conn keyspace table-name
                                                        (where [[= :id 0]]))
                                    first
                                    :count)]
                     (assoc op :type :ok :value value)))))

  (teardown! [this test]))

(def add {:type :invoke :f :add :value 1})
(def sub {:type :invoke :f :add :value -1})
(def r   {:type :invoke :f :read})

(defn test-inc
  [opts]
  (yugabyte-test
    (merge opts
           {:name             "cql-counter-inc"
            :client           (CQLCounterClient. nil)
            :client-generator (->> (repeat 100 add)
                                   (cons r)
                                   gen/mix
                                   (gen/delay 1/10))
            :model            nil
            :checker         (checker/compose
                               {:perf     (checker/perf)
                                :timeline (timeline/html)
                                :counter  (checker/counter)})})))

(defn test-inc-dec
  [opts]
  (yugabyte-test
    (merge opts
           {:name             "cql-counter-inc-dec"
            :client           (CQLCounterClient. nil)
            :client-generator (->> (take 100 (cycle [add sub]))
                                   (cons r)
                                   gen/mix
                                   (gen/delay 1/10))
            :model            nil
            :checker          (checker/compose
                                {:perf     (checker/perf)
                                 :timeline (timeline/html)
                                 :counter  (checker/counter)})})))
