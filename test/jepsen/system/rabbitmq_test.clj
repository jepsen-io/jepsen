(ns jepsen.system.rabbitmq-test
  (:use jepsen.system.rabbitmq
        jepsen.core
        jepsen.core-test
        clojure.test
        clojure.pprint)
  (:require [jepsen.os.debian :as debian]
            [jepsen.model     :as model]
            [jepsen.generator :as gen]))

(deftest rabbit-test
  (let [test (run! (assoc noop-test
                          :os         debian/os
                          :db         db
                          :client     (queue-client)
                          :model      (model/unordered-queue)
                          :generator  (->> gen/queue
                                           (gen/finite-count 1000)
                                           (gen/nemesis gen/void))))]
    (is (:valid? (:results test)))
    (pprint test)))
