(ns jepsen.ignite.register
  "Single atomic register test"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging   :refer :all]
            [jepsen [ignite          :as ignite]
                    [checker         :as checker]
                    [client          :as client]
                    [nemesis         :as nemesis]
                    [independent     :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model           :as model])
  (:import (org.apache.ignite Ignition IgniteCache)
           (clojure.lang ExceptionInfo)))

(def key "k")
(def cache-name "REGISTER")

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord Client [conn cache-config]
  client/Client
  (open! [this test node]
         (let [config (ignite/configure-client (:nodes test) (:pds test))
               conn (Ignition/start (.getCanonicalPath config))]
              (.getOrCreateCache conn cache-config)
              (assoc this :conn conn)))

  (setup! [this test])

  (invoke! [_ test op]
    (try
      (case (:f op)
            :read (let [cache (.cache conn cache-name)
                        value (.get cache key)]
                       (assoc op :type :ok :value value))
            :write (let [cache (.cache conn cache-name)]
                        (.put cache key (:value op))
                        (assoc op :type :ok))
            :cas (let [cache (.cache conn cache-name)
                       [value value'] (:value op)]
                      (assoc op :type (if (.replace cache key value value') :ok :fail))))))

  (teardown! [this test])

  (close! [_ test]
    (.destroy (.cache conn cache-name))
    (.close conn)))

(defn test
  [opts]
  (ignite/basic-test
    (merge
      {:name      "register-test"
       :client    (Client. nil (ignite/get-cache-config opts cache-name))
       :checker   (independent/checker
                    (checker/compose
                      {:linearizable (checker/linearizable {:model (model/cas-register)})
                       :timeline  (timeline/html)}))
       :generator (ignite/generator [r w cas] (:time-limit opts))}
      opts)))
