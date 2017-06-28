(ns jepsen.control-test
  (:require [jepsen.control :as c]
            [clojure.test :refer :all]))

(deftest basic-test
  (c/with-ssh {}
    (c/on "n1"
          (is (= (c/exec :whoami) "root")))))
