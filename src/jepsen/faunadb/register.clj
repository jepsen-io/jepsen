(ns jepsen.faunadb.register
  "Simulates updates to a field on an instance"
  (:refer-clojure :exclude [test])
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [fauna :as fauna]
                    [generator :as gen]
                    [independent :as indy]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.faunadb [client :as f]
                            [query :as q]]
            [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn instanceRef
  "The instance under test."
  [id]
  (q/ref "test" id))

(def register
  "Path to the register."
  (q/expr ["data" "register"]))

(def RegisterField
  ; TODO: how is this different from the register path?
  (.to (Field/at (into-array String ["data" "register"])) Codec/LONG))

(defrecord AtomicClient [tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/linearized-client node)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (f/query conn (q/create-class {:name "test"})))))

  (invoke! [this test op]
    ; TODO: destructuring bind
    (let [[id val'] (:value op)
          id*  (q/ref "test" id)]
      (case (:f op)
        :read (let [val (f/query conn
                                 (q/if (q/exists? id*)
                                   (q/select ["data" "register"]
                                             (q/get id*))))]
                (assoc op
                       :type :ok
                       :value (indy/tuple id (and val
                                                  (.get val f/LongField)))))

        :write (do (-> conn
                       (f/queryGet
                         (q/if (q/exists? id*)
                           (q/update id* {:data {:register val'}})
                           (q/create id* {:data {:register val'}}))
                         RegisterField))
                   (assoc op :type :ok))

        :cas (let [[expected new] val'
                   cas (f/queryGet
                         conn
                         (q/if (q/exists? id*)
                           (q/let [reg (q/select ["data" "register"]
                                                 (q/get id*))]
                             (q/if (q/= expected reg)
                               (q/do
                                 (q/update id* {:data {:register new}})
                                 true)
                               false))
                           false)
                         f/BoolField)]
               (assoc op :type (if cas :ok :fail))))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn register-test-base
  [opts]
  (fauna/basic-test
   (merge
    {:client {:client (:client opts)
              :during (indy/concurrent-generator
                       10
                       (range)
                       (fn [k]
                         (->> (gen/reserve 5 (gen/mix [w cas cas]) r)
                              (gen/delay-til 1/2)
                              (gen/stagger 0.1)
                              (gen/limit 100))))}
     :checker (checker/compose
               {:perf    (checker/perf)
                :details (indy/checker
                          (checker/compose
                           {:timeline     (timeline/html)
                            :linearizable (checker/linearizable)}))})}
    (dissoc opts :client))))

(defn test
  [opts]
  (register-test-base
   (merge {:name "register"
           :model (model/cas-register 0)
           :client (AtomicClient. (atom false) nil)}
          opts)))

