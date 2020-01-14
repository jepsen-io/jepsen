(ns jepsen.consul.register
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]
             [independent :as independent]]
            [jepsen.control.net :as net]
            [jepsen.checker.timeline :as timeline]
            [jepsen.consul.client :as c]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]
            [cheshire.core :as json])
  (:import (knossos.model Model)))

(defrecord Client [client]
  client/Client
  (open! [this test node]
    (let [client (str "http://" (net/ip (name node)) ":8500/v1/kv/")]
      (assoc this :client client)))

  (invoke! [_ test op]
    (let [[k value] (:value op)
          crash (if (= :read (:f op)) :fail :info)]
      (c/with-errors op #{:read}
        (case (:f op)
          :read (let [v (-> client
                            (c/get k)
                            c/parse
                            :value)]
                  (assoc op :type :ok :value (independent/tuple k v)))

          :write (let [v (->> value
                              json/generate-string
                              (c/put! client k))]
                   (assoc op :type :ok))

          :cas   (let [[value value'] value]
                   (assoc op :type (if (c/cas! client
                                               k
                                               (json/generate-string value)
                                               (json/generate-string value'))
                                     :ok
                                     :fail)))))))

  ;; HTTP clients are stateless
  (close! [_ _])
  (setup! [_ _])
  (teardown! [_ _]))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  {:client  (Client. nil)
   :checker (independent/checker
             (checker/compose
              {:linear   (checker/linearizable
                          {:model (model/cas-register)})
               :timeline (timeline/html)}))
   :generator (independent/concurrent-generator
               10
               (range)
               (fn [k]
                 (->> (gen/mix [w cas])
                      (gen/reserve 5 r)
                      (gen/limit (:ops-per-key opts)))))})
