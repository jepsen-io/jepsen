(ns aerospike.cas-register
  "Compare-and-set register test"
  (:require [aerospike.support :as s]
            [jepsen
             [client    :as client]
             [checker   :as checker]
             [generator :as gen]
             [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [wall.hack]))

(defrecord CasRegisterClient [client namespace set]
  client/Client
  (open! [this test node]
    (assoc this :client (s/connect node)))

  (setup! [this test])

  (invoke! [this test op]
    (s/with-modern-errors op
      (let [[k v] (:value op)]
        (case (:f op)
          :read (assoc op
                       :type :ok,
                       :value (independent/tuple
                               k (-> client (s/fetch namespace set k)
                                     :bins :value)))

          :cas   (let [[v v'] v]
                   (s/cas! client namespace set k
                           (fn [r]
                           ; Verify that the current value is what we're cas'ing
                           ; from
                             (when (not= v (:value r))
                               (throw (ex-info "skipping cas" {})))
                             {:value v'}))
                   (assoc op :type :ok))

          :write (do (s/put! client namespace set k {:value v})
                     (assoc op :type :ok))))))

  (teardown! [this test])

  (close! [this test]
    (s/close client)))

(defn cas-register-client
  "A basic CAS register on top of a single key and bin."
  []
  (CasRegisterClient. nil s/ans "cats"))

; Generators

(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn add [_ _] {:type :invoke, :f :add, :value 1})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn linear-checker-args
  []
  {:algorithm :linear
   :model (model/cas-register)})

(defn workload
  []
  {:client  (cas-register-client)
   :checker (independent/checker
             (checker/compose
              {:linear   (checker/linearizable (linear-checker-args))
               :timeline (timeline/html)}))
   :model (model/cas-register)
   :generator (independent/concurrent-generator
               10
               (range)
               (fn [k]
                 (->> (gen/reserve 5 r (gen/mix [w cas cas]))
                      (gen/stagger 1)
                      (gen/limit (+ 100 (rand-int 100))))))})
