(ns jepsen.system.elasticsearch-test
  (:use jepsen.system.elasticsearch
        jepsen.core
        jepsen.tests
        clojure.test
        clojure.pprint)
  (:require [clojure.string   :as str]
            [jepsen.util      :as util]
            [jepsen.os.debian :as debian]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.model     :as model]
            [jepsen.generator :as gen]
            [jepsen.nemesis   :as nemesis]
            [jepsen.store     :as store]
            [jepsen.report    :as report]))

;(deftest register-test
;  (let [test (run!
;               (assoc
;                 noop-test
;                 :name      "elasticsearch"
;                 :os        debian/os
;                 :db        db
;                 :client    (cas-set-client)
;                 :model     (model/set)
;                 :checker   (checker/compose {:html timeline/html
;                                              :set  checker/set})
;                 :nemesis   (nemesis/partitioner nemesis/bridge)
;                 :generator (gen/phases
;                              (->> (range)
;                                   (map (fn [x] {:type  :invoke
;                                                 :f     :add
;                                                 :value x}))
;                                   gen/seq
;                                   (gen/stagger 1/10)
;                                   (gen/delay 1)
;                                   (gen/nemesis
;                                     (gen/seq
;                                       (cycle [(gen/sleep 60)
;                                               {:type :info :f :start}
;                                               (gen/sleep 300)
;                                               {:type :info :f :stop}])))
;                                   (gen/time-limit 600))
;                              (gen/nemesis
;                                (gen/once {:type :info :f :stop}))
;                              (gen/clients
;                                (gen/once {:type :invoke :f :read})))))]
;    (is (:valid? (:results test)))
;    (pprint (:results test))))

(deftest create-test
  (let [test (run!
               (assoc
                 noop-test
                 :name      "elasticsearch"
                 :os        debian/os
                 :db        db
                 :client    (create-set-client)
                 :model     (model/set)
                 :checker   (checker/compose {:html timeline/html
                                              :set  checker/set})
                 :nemesis   isolate-self-primaries-nemesis
                 ;:nemesis   (nemesis/partition-random-node)
                 ;:nemesis   (nemesis/partition-random-halves)
                 ;:nemesis   (nemesis/partition-halves)
                 ;:nemesis   (nemesis/partitioner nemesis/bridge)
                 :generator (gen/phases
                              (->> (range)
                                   (map (fn [x] {:type  :invoke
                                                 :f     :add
                                                 :value x}))
                                   gen/seq
                                   (gen/stagger 1/10)
                                   (gen/delay 1)
                                   (gen/nemesis
                                     (gen/seq
                                       (cycle
                                         [(gen/sleep 30)
                                          {:type :info :f :start}
                                          (gen/sleep 200)
                                          {:type :info :f :stop}])))
                                   (gen/time-limit 400))
                              (gen/nemesis
                                (gen/once {:type :info :f :stop}))
                              (gen/clients
                                (gen/once {:type :invoke :f :read})))))]
    (is (:valid? (:results test)))
    (pprint (:results test))))
