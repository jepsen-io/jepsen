(ns jepsen.control.core-test
  (:require [clojure [test :refer :all]]
            [jepsen.control.core :refer :all]))

(deftest escape-test
  (is (= "start --data-dir \"/opt/foo/\\\"data\\\"\" --auth-method password  "
         (escape (seq [:start :--data-dir "/opt/foo/\"data\"" :--auth-method "password"
                       nil nil])))))
