(ns jepsen.core-test
  (:use jepsen.core
        clojure.test
        clojure.pprint)
  (:require [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.model :as model]
            [jepsen.checker :as checker]))

(def noop-os (reify os/OS
               (setup!    [os test node])
               (teardown! [os test node])))

(defn atom-db
  "Wraps an atom as a database."
  [state]
  (reify db/DB
    (setup!    [db test node] (reset! state 0))
    (teardown! [db test node] (reset! state :done))))

(defn atom-client
  "A CAS client which uses an atom for state."
  [state]
  (reify client/Client
    (setup!    [this test node] this)
    (teardown! [this test node])
    (invoke!   [this test op]
      (case (:f op)
        :write (do (reset! state   (:value op))
                   (assoc op :type :ok))

        :cas   (let [[cur new] (:value op)]
                 (try
                   (swap! state (fn [v]
                                  (if (= v cur)
                                    new
                                    (throw (RuntimeException. "CAS failed")))))
                   (assoc op :type :ok)
                   (catch RuntimeException e
                     (assoc op :type :fail))))

        :read  (assoc op :type :ok
                      :value @state)))))

(def noop-nemesis
  (reify client/Client
    (setup!    [this test node] this)
    (teardown! [this test node])
    (invoke!   [this test op])))

(deftest basic-cas-test
  (let [state (atom nil)
        db    (atom-db state)
        n     100000
        test  (run! {:nodes      [nil nil nil nil nil]
                     :os         noop-os
                     :db         (atom-db state)
                     :client     (atom-client state)
                     :nemesis    noop-nemesis
                     :generator  (->> gen/cas
                                      (gen/finite-count n)
                                      (gen/nemesis gen/void))
                     :model      (model/->CASRegister 0)
                     :checker    checker/linearizable})]
    (pprint test)
    (is (:valid? (:results test)))))
