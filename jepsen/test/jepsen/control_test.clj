(ns jepsen.control-test
  (:require [jepsen [control :as c]
                    [common-test :refer [quiet-logging]]
                    [util :refer [contains-many?]]]
            [jepsen.control.sshj :as sshj]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.test :refer :all]))

(use-fixtures :once quiet-logging)

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

(defn test-remote
  [remote]
  (binding [jepsen.control/*remote* remote]
    (testing "simple exec"
      (c/with-ssh {}
        (c/on "n1"
              (is (= (c/exec :whoami) "root")))))

    (testing "On failure, throws debug data."
      (try+
        (c/with-ssh {}
          (c/on "n1" (c/exec :thiscmdshouldnotexist)))
        (catch [:type :jepsen.control/nonzero-exit] e
          (is (= :jepsen.control/nonzero-exit (:type e)))
          (is (= "n1"   (:host e)))
          (is (= "foo"  (:cmd e)))
          (is (= "a"    (:out e)))
          (is (= "f"    (:err e)))
          (is (= 5      (:exit e))))))))

(deftest ^:integration sshj-remote-test
  (test-remote (sshj/remote)))
