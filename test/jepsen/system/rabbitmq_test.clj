(ns jepsen.system.rabbitmq-test
  (:use jepsen.system.rabbitmq
        jepsen.core
        jepsen.core-test
        clojure.test
        clojure.pprint)
  (:require [jepsen.os.debian :as debian]
            [jepsen.checker   :as checker]
            [jepsen.model     :as model]
            [jepsen.generator :as gen]
            [jepsen.nemesis   :as nemesis]
            [jepsen.store     :as store]))

(deftest rabbit-test
  (let [test (run!
               (assoc
                 noop-test
                 :name       "rabbitmq-simple-partition"
                 :os         debian/os
                 :db         db
                 :client     (queue-client)
                 :nemesis    (nemesis/partition-random-halves)
                 :model      (model/unordered-queue)
                 :checker    (checker/compose
                               {:queue       checker/queue
                                :total-queue checker/total-queue})
                 :generator  (gen/phases
                               (->> (gen/queue)
                                    (gen/delay 1)
                                    (gen/nemesis
                                      (gen/seq
                                        (cycle [(gen/sleep 30)
                                                {:type :info :f :start}
                                                (gen/sleep 30)
                                                {:type :info :f :stop}])))
                                    (gen/time-limit 1000))
                               (gen/nemesis
                                 (gen/once {:type :info, :f :stop}))
                               (gen/log "waiting for recovery")
                               (gen/sleep 60)
                               (gen/clients
                                 (gen/each {:type :invoke
                                            :f    :drain})))))]
    (is (:valid? (:results test)))
    (pprint (:results test))))
