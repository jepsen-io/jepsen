(ns jepsen.zookeeper-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.zookeeper :as zk]))

(deftest zk-test
  (is (:valid? (:results (jepsen/run! (zk/zk-test "3.4.5+dfsg-2"))))))
