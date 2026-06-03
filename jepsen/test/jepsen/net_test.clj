(ns jepsen.net-test
  (:require [clojure [test :refer :all]]
            [jepsen [control :as c]
                    [net :as net]]))

(deftest net-dev-test
  (c/on "n1"
    (is (= "eth0" (net/net-dev)))
    (is (= "eth0" (net/net-dev)))))
