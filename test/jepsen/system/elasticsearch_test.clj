(ns jepsen.system.elasticsearch-test
  (:use jepsen.system.elasticsearch
        jepsen.core
        jepsen.core-test
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

(deftest register-test
  (let [test (run!
               (assoc
                 noop-test
                 :name      "elasticsearch"
                 :os        debian/os
                 :db        db
                 :client    (set-client)
                 :model     (model/set)
                 :checker   (checker/compose {:html   timeline/html
                                              :linear checker/linearizable})
                 :nemesis   (nemesis/partition-random-halves)
                 :generator (gen/phases
                              (->> (range)
                                   (map (fn [x] {:type  :invoke
                                                 :f     :add
                                                 :value x}))
                                   gen/seq
                                   (gen/delay 1)
                                   (gen/nemesis
                                     (gen/seq
                                       (cycle [(gen/sleep 30)
                                               {:type :info :f :start}
                                               (gen/sleep 30)
                                               {:type :info :f :stop}])))
                                   (gen/time-limit 120))
                              (gen/nemesis
                                (gen/once {:type :info :f :stop}))
                              (gen/log "waiting for recovery")
                              (gen/sleep 30)
                              (gen/clients
                                (gen/once {:type :invoke :f :read})))))]
    (is (:valid? (:results test)))
    (report/linearizability (:linear (:results test)))))
