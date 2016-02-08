(ns jepsen.cockroach-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.control :as control]
            [jepsen.cockroach :as cl]))

(def nodes [:n1l :n2l :n3l :n4l :n5l])

(deftest atomic-test  (is (:valid? (:results (jepsen/run! (cl/atomic-test nodes))))))

(deftest sets-test  (is (:valid? (:results (jepsen/run! (cl/sets-test nodes))))))

(deftest monotonic-test  (is (:valid? (:results (jepsen/run! (cl/monotonic-add-test nodes))))))

(deftest bank-test  (is (:valid? (:results (jepsen/run! (cl/bank-test nodes 4 10))))))
