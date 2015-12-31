(ns jepsen.zookeeper-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.zookeeper :as zk]))

(deftest simple-test
  (is (:valid? (:results (jepsen/run! (zk/simple-test "3.4.5+dfsg-2"))))))
