(ns jepsen.rabbitmq-test
  (:use jepsen.rabbitmq
        jepsen.core
        jepsen.tests
        clojure.test
        clojure.pprint)
  (:require [clojure.string   :as str]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.core :as jepsen]
            [jepsen.generator :as gen]
            [jepsen.nemesis   :as nemesis]
            [jepsen.os.debian :as debian]
            [jepsen.store     :as store]
            [jepsen.report    :as report]
            [jepsen.util      :as util]
            [knossos.model    :as model]))

;(deftest mutex-test
;  (let [test (run!
;               (assoc
;                 noop-test
;                 :name      "rabbitmq-mutex"
;                 :os        debian/os
;                 :db        db
;                 :client    (mutex)
;                 :checker   (checker/compose {:html   timeline/html
;                                              :linear checker/linearizable})
;                 :model     (model/mutex)
;                 :nemesis   (nemesis/partition-random-halves)
;                 :generator (gen/phases
;                              (->> (gen/seq
;                                     (cycle [{:type :invoke :f :acquire}
;                                             {:type :invoke :f :release}]))
;                                gen/each
;                                (gen/delay 180)
;                                (gen/nemesis
;                                  (gen/seq
;                                    (cycle [(gen/sleep 5)
;                                            {:type :info :f :start}
;                                            (gen/sleep 100)
;                                            {:type :info :f :stop}])))
;                                (gen/time-limit 500)))))]
;    (is (:valid? (:results test)))
;    (report/linearizability (:linear (:results test)))))

(deftest rabbit-test
  (let [test (jepsen/run!
               (assoc
                 noop-test
                 :name       "rabbitmq-simple-partition"
                 :os         debian/os
                 :db         db
                 :client     (queue-client)
                 :nemesis    (nemesis/partition-random-halves)
                 :model      (model/unordered-queue)
                 :checker    (checker/compose
                               {:queue       (checker/queue)
                                :total-queue (checker/total-queue)})
                 :generator  (gen/phases
                               (->> (gen/queue)
                                    (gen/delay 1/10)
                                    (gen/nemesis
                                      (gen/seq
                                        (cycle [(gen/sleep 60)
                                                {:type :info :f :start}
                                                (gen/sleep 60)
                                                {:type :info :f :stop}])))
                                    (gen/time-limit 360))
                               (gen/nemesis
                                 (gen/once {:type :info, :f :stop}))
                               (gen/log "waiting for recovery")
                               (gen/sleep 60)
                               (gen/clients
                                 (gen/each
                                   (gen/once {:type :invoke, :f :drain}))))))]
    (is (:valid? (:results test)))))
