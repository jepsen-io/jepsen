(ns jepsen.zookeeper-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.zookeeper :refer :all]))

(deftest install-test
  (is (:valid? (:results (run! (simple-test "3.4.5+dfsg-2"))))))
