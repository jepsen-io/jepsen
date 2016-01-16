(ns jepsen.cockroach-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.cockroach :as cl]))

;(deftest cl-test
;  (is (:valid? (:results (jepsen/run! (cl/simple-test "hello-version"))))))
(deftest cl-test
  (is (:valid? (:results (jepsen/run! (cl/client-test "hello-version"))))))

