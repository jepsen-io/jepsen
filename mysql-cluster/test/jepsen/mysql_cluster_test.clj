(ns jepsen.mysql-cluster-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.mysql-cluster :refer :all]))

(deftest simple-test-test
  (is (:valid? (:results (run! (simple-test "7.4.7"))))))
