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

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 1000)})

(defrecord Client [conn cache config]
  client/Client
  (open! [this test node]
    (let [config (ignite/configure-client (:nodes test))
      conn (Ignition/start (.getCanonicalPath config))
      cache (.getOrCreateCache conn "JepsenCache")]
      (assoc this :conn conn :cache cache :config config)))

  (setup! [this test])

  (invoke! [_ test op]
    (try
      (case (:f op)
        :read (let [value (.get cache "k")]
                (assoc op :type :ok :value value))
        :write (do (.put cache "k" (:value op))
                (assoc op :type :ok)))))

  (teardown! [this test]
    (.delete config))

  (close! [_ test]
    (.stop conn true)))

(defn test
  [opts]
  (ignite/basic-test
    (merge
      {:name      "register-test"
       :client    (Client. nil nil nil)
       :checker   (independent/checker
                    (checker/compose
                      {:linearizable (checker/linearizable {:model (model/cas-register)})
                       :timeline  (timeline/html)}))
       :generator (ignite/generator [r w] (:time-limit opts))}
      opts)))
