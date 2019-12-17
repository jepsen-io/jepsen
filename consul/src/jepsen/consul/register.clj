(ns jepsen.consul.register
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]
             [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.consul.client :as c]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]])
  (:import (knossos.model Model)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/client node)))

  (setup! [this test])

  (invoke! [_ test op]
    (let [[k [version value]] (:value op)]
      (c/with-errors op #{:read}
        (case (:f op)
          :read (let [r (c/get conn k {:serializable? (:serializable test)})
                      v [(:version r) (:value r)]]
                  (assoc op :type :ok, :value (independent/tuple k v)))

          :write (let [r        (c/put! conn k value)
                       version  (-> r :prev-kv val :version inc)]
                   (assoc op
                          :type :ok
                          :value (independent/tuple k [version value])))

          :cas (let [[old new]  value
                     r          (c/cas*! conn k old new)
                     version    (some-> r :puts first :prev-kv val :version
                                        inc)]
                 (if (:succeeded? r)
                   (assoc op
                          :type  :ok
                          :value (independent/tuple k [version value]))
                   (assoc op :type :fail)))))))

  (teardown! [this test])

  (close! [_ test]
    (c/close! conn)))

(defn r   [_ _] {:type :invoke, :f :read, :value [nil nil]})
(defn w   [_ _] {:type :invoke, :f :write, :value [nil (rand-int 5)]})
(defn cas [_ _] {:type :invoke, :f :cas, :value [nil [(rand-int 5) (rand-int 5)]]})

(defn workload
  "Tests linearizable reads, writes, and compare-and-set operations on
  independent keys."
  [opts]
  {:client    (Client. nil)
   :checker   (independent/checker
               (checker/compose
                {:linear   (checker/linearizable
                            {:model (->VersionedRegister 0 nil)})
                 :timeline (timeline/html)}))
   :generator (independent/concurrent-generator
               10
               (range)
               (fn [k]
                 (->> (gen/mix [w cas])
                      (gen/reserve 5 r)
                      (gen/limit (:ops-per-key opts)))))})
