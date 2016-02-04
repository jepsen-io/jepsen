(ns jepsen.cockroach-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.control :as control]
            [jepsen.cockroach :as cl]))

;(deftest atomic-test  (is (:valid? (:results (jepsen/run! (cl/atomic-test "hello-version"))))))
(deftest sets-test  (is (:valid? (:results (jepsen/run! (cl/sets-test "hello-version"))))))

