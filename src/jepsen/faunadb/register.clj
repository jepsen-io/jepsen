(ns jepsen.faunadb.register
  "Simulates updates to a field on an instance"
  (:refer-clojure :exclude [test])
  (:use jepsen.faunadb.query)
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [fauna :as fauna]
                    [generator :as gen]
                    [independent :as indy]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.faunadb.client :as f]
            [clojure.string :as str]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn instanceRef
  "The instance under test."
  [id]
  (Ref (ClassRef (v "test")) id))

(def register
  "Path to the register."
  (Arr (v "data") (v "register")))

(def RegisterField
  (.to (Field/at (into-array String ["data" "register"])) Codec/LONG))

(defrecord AtomicClient [tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (f/query conn (CreateClass (Obj "name" (v "test")))))))

  (invoke! [this test op]
    (let [id   (key (:value op))
          val' (val (:value op))]
                              
      (case (:f op)
        :read (let [val (-> conn
                            (f/query
                             (If (Exists (instanceRef id))
                                 (Select register (Get (instanceRef id)))
                                 Null)))]
                (assoc op :type :ok :value (indy/tuple id (if (nil? val) nil (.get val f/LongField)))))

        :write (do
                 (-> conn
                     (f/queryGet
                      (If (Exists (instanceRef id))
                        (Update (instanceRef id)
                                (Obj "data" (Obj "register" val')))
                        (Create (instanceRef id)
                                (Obj "data" (Obj "register" val'))))
                      RegisterField))
                 (assoc op :type :ok))
        :cas (let [[expected new] val'
                   cas (f/queryGet conn
                                (If (Exists (instanceRef id))
                                    (Let
                                     {"reg" (Select register (Get (instanceRef id)))}
                                     (If (Equals (v expected) (Var "reg"))
                                         (Do
                                          (Update (instanceRef id)
                                                  (Obj "data" (Obj "register" new)))
                                          (v true))
                                         (v false)))
                                    (v true)) ; instance not found is ok - create may not have happened yet
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
  
