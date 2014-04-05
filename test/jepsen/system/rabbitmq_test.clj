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
            [jepsen.nemesis   :as nemesis]))

(deftest rabbit-test
  (let [test (run!
               (assoc
                 noop-test
                 :os         debian/os
                 :db         db
                 :client     (queue-client)
                 :nemesis    (nemesis/simple-partition)
                 :model      (model/unordered-queue)
                 :checker    (checker/compose
                               {:queue       checker/queue
                                :total-queue checker/total-queue})
                 :generator  (->> (gen/queue)
                                  (gen/delay 0.1)
                                  (gen/time-limit 2500)
                                  (gen/then (gen/sleep 10))
                                  (gen/then (gen/each {:type :invoke
                                                       :f    :drain}))
                                  (gen/nemesis
                                    (gen/concat
                                      (gen/start-stop 100 100)
                                      (gen/sleep 100)
                                      (gen/start-stop 0 100)
                                      (gen/sleep 100)
                                      (gen/start-stop 0 100)
                                      (gen/sleep 100)
                                      (gen/start-stop 0 100)
                                      (gen/sleep 100)
                                      (gen/start-stop 0 100)
                                      (gen/sleep 100)
                                      (gen/start-stop 0 100)
                                      (gen/sleep 100)
                                      (gen/start-stop 0 100)
                                      (gen/sleep 100)
                                      (gen/start-stop 0 100)
                                      (gen/sleep 100)
                                      (gen/start-stop 0 100)
                                      (gen/sleep 100)
                                      (gen/start-stop 0 100)
                                      (gen/sleep 100)
                                                      )))))]

    (is (:valid? (:results test)))
    (pprint (:results test))))
