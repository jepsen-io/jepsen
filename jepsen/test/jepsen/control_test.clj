(ns jepsen.control-test
  (:require [jepsen.control :as c]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.test :refer :all]
            [jepsen.util :refer [contains-many?]]))

(deftest ^:integration session-test
  (testing "on failure, session throws debug data"
    (try+
     (c/with-ssh {}
       (c/on "thishostshouldnotresolve"
             (c/exec :echo "hello")))
     (catch Object m
       (is (contains-many? m :dir :username :port :host))))))

(deftest ^:integration exec-test
  (testing "simple exec"
    (c/with-ssh {}
      (c/on "n1"
            (is (= (c/exec :whoami) "root")))))

  (testing "on failure, exec throws debug data"
    (try+
     (c/with-ssh {}
       (c/on "n1" (c/exec :thiscmdshouldnotexist)))
     (catch Object m
       (is (contains-many? m :cmd :out :err :host :exit))))))
