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

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord Client [conn cache config cache-config]
  client/Client
  (open! [this test node]
    (let [config (ignite/configure-client (:nodes test) (:pds test))
      conn (Ignition/start (.getCanonicalPath config))
      cache (.getOrCreateCache conn (ignite/getCacheConfiguration cache-config))]
      (assoc this :conn conn :cache cache :config config)))

  (setup! [this test])

  (invoke! [_ test op]
    (try
      (case (:f op)
        :read (let [value (.get cache key)]
                (assoc op :type :ok :value value))
        :write (do (.put cache key (:value op))
                (assoc op :type :ok))
        :cas (let [[value value'] (:value op)]
                (assoc op :type (if (.replace cache key value value') :ok :fail))))))

  (teardown! [this test]
    (.delete config))

  (close! [_ test]
    (.close conn)))

(defn test
  [opts]
  (ignite/basic-test
    (merge
      {:name      "register-test"
       :client    (Client. nil nil nil (ignite/get-cache-config opts))
       :checker   (independent/checker
                    (checker/compose
                      {:linearizable (checker/linearizable {:model (model/cas-register)})
                       :timeline  (timeline/html)}))
       :generator (ignite/generator [r w cas] (:time-limit opts))}
      opts)))
