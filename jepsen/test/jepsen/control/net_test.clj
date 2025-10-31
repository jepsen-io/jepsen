(ns jepsen.control.net-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [control :as c]]
            [jepsen.control.net :as cn]))

(deftest ip-by-local-dns-test
  (is (= "72.14.179.192" (cn/ip-by-local-dns "aphyr.com"))))

(deftest ^:integration ip-by-remote-getent-test
  (c/on "n1"
    (is (= "72.14.179.192" (cn/ip-by-remote-getent "aphyr.com")))))

(deftest ip-test
  (is (= "72.14.179.192" (cn/ip "aphyr.com"))))
