(ns jepsen.mongodb-rocks-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.mongodb-rocks :refer :all]))

(deftest logger-perf
  (is (:valid? (:results (run! (logger-perf-test))))))
