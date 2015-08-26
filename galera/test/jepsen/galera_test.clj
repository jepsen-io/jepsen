(ns jepsen.galera-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.galera :refer :all]))

(deftest simple-test-test
  (is (:valid? (:results (run! (simple-test "7.4.7"))))))
