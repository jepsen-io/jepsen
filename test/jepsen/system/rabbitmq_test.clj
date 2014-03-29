(ns jepsen.system.rabbitmq-test
  (:use jepsen.system.rabbitmq
        jepsen.core
        jepsen.core-test
        clojure.test)
  (:require [jepsen.os.debian :as debian]))

(deftest rabbit-test
  (let [test (run! (assoc noop-test
                          :os debian/os
                          :db db))]
    (is (:valid? (:results test)))))
