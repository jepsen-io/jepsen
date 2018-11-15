(ns jepsen.faunadb.register
  "Simulates updates to a field on an instance"
  (:refer-clojure :exclude [test])
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:require [jepsen [checker :as checker]
                    [client :as client]
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

(defrecord AtomicClient [tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/linearized-client node)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (f/query conn (q/create-class {:name "test"})))))

  (invoke! [this test op]
    (f/with-errors op #{:read}
      (let [[id val'] (:value op)
            id*  (q/ref "test" id)]
        (case (:f op)
          :read (let [v (f/query conn
                                 (q/if (q/exists? id*)
                                   (q/select ["data" "register"]
                                             (q/get id*))))]
                  (assoc op
                         :type :ok
                         :value (indy/tuple id v)))

          :write (do (-> conn
                         (f/query
                           (q/if (q/exists? id*)
                             (q/update id* {:data {:register val'}})
                             (q/create id* {:data {:register val'}}))))
                     (assoc op :type :ok))

          :cas (let [[expected new] val'
                     cas (f/query
                           conn
                           (q/if (q/exists? id*)
                             (q/let [reg (q/select ["data" "register"]
                                                   (q/get id*))]
                               (q/if (q/= expected reg)
                                 (q/do
                                   (q/update id* {:data {:register new}})
                                   true)
                                 false))
                             false))]
                 (assoc op :type (if cas :ok :fail)))))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn workload
  [opts]
  (let [n (count (:nodes opts))]
    {:client    (AtomicClient. (atom false) nil)
     :model     (model/cas-register 0)
     :generator (indy/concurrent-generator
                  (* 2 n)
                  (range)
                  (fn [k]
                    (->> (gen/reserve n (gen/mix [w cas cas]) r)
                         (gen/delay-til 1/2)
                         (gen/stagger 0.1)
                         (gen/limit 100))))
     :checker (indy/checker
                (checker/compose
                  {:timeline     (timeline/html)
                   :linearizable (checker/linearizable)}))}))
