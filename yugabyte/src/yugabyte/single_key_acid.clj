(ns yugabyte.single-key-acid
  (:require [clojure [pprint :refer :all]]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [clojurewerkz.cassaforte [client :as cassandra]
                                     [query :refer :all]
                                     [policies :refer :all]
                                     [cql :as cql]]
            [yugabyte [auto :as auto]
                      [client :as c]
                      [core :refer :all]]))

(def keyspace "jepsen")
(def table-name "single_key_acid")
(def setup-lock (Object.))

(c/defclient CQLSingleKey []
  (setup! [this test]
    (locking setup-lock
      (cql/create-keyspace conn keyspace
                           (if-not-exists)
                           (with {:replication
                                  {"class" "SimpleStrategy"
                                   "replication_factor" 3}}))
      (cql/use-keyspace conn keyspace)
      (cql/create-table conn table-name
                        (if-not-exists)
                        (column-definitions {:id :int
                                             :val :int
                                             :primary-key [:id]}))))

  (invoke! [this test op]
    (c/with-errors op #{:read}
      (let [[id val] (:value op)]
        (case (:f op)
          :write
          (do (cql/insert-with-ks conn keyspace table-name
                                  {:id id, :val val})
              (assoc op :type :ok))

          :cas
          (let [[expected-val new-val] val
                res (cql/update-with-ks conn keyspace table-name
                                        {:val new-val}
                                        (only-if [[= :val expected-val]])
                                        (where [[= :id id]]))
                applied (get (first res) (keyword "[applied]"))]
            (assoc op :type (if applied :ok :fail)))

          :read
          (let [value (->> (cql/select-with-ks conn keyspace table-name
                                               (where [[= :id id]]))
                           first
                           :val)]
            (assoc op :type :ok :value (independent/tuple id value)))))))

  (teardown! [this test]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn test
  [opts]
  (let [n (count (:nodes opts))]
    (yugabyte-test
      (merge opts
             {:name "singe-key-acid"
              :client (CQLSingleKey. nil)
              :concurrency (max 10 (:concurrency opts))
              :client-generator (independent/concurrent-generator
                                  (* 2 n)
                                  (range)
                                  (fn [k]
                                    (->> (gen/reserve n (gen/mix [w cas cas]) r)
                                         (gen/stagger 0.1)
                                         (gen/limit 100))))
              :model (model/cas-register 0)
              :checker (checker/compose
                         {:perf (checker/perf)
                          :indep (independent/checker
                                   (checker/compose
                                     {:timeline (timeline/html)
                                      :linear   (checker/linearizable)}))})}))))
