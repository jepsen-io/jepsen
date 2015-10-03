(ns jepsen.zookeeper.ephemeral-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.zookeeper.ephemeral :refer :all]))

(deftest ephemeral-test'
  (is (:valid? (:results (run! (ephemeral-test "3.4.5+dfsg-2"))))))
