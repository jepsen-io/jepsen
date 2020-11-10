(ns jepsen.control-test
  (:require [jepsen [control :as c]
                    [common-test :refer [quiet-logging]]
                    [util :refer [contains-many?]]]
            [jepsen.control.sshj :as sshj]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.java.io :as io]
            [clojure.test :refer :all])
  (:import (java.io File)))

(use-fixtures :once quiet-logging)

(def test-resource
  "A file in /resources that we can use to verify uploads/downloads."
  "bump-time.c")

(defn test-remote
  [remote]
  (c/with-ssh {}
    (binding [jepsen.control/*remote* remote]
      (testing "on failure, session throws debug data"
        (try+
          (c/on "thishostshouldnotresolve"
                (c/exec :echo "hello"))
          (catch [:type :jepsen.control/session-error] e
            (is (= "Error opening SSH session. Verify username, password, and node hostnames are correct." (:message e)))
            (is (= "thishostshouldnotresolve" (:host e)))
            (is (= 22     (:port e)))
            (is (= "root" (:username e)))
            (is (= "root" (:password e))))))

      (c/on "n1"
        (testing "simple exec"
          (is (= (c/exec :whoami) "root")))

        (testing "on failure, throws debug data."
          (try+
            (c/exec :thiscmdshouldnotexist)
            (catch [:type :jepsen.control/nonzero-exit] e
              (is (= "n1" (:host e)))
              (is (= "cd /; thiscmdshouldnotexist" (:cmd e)))
              (is (= "" (:out e)))
              (is (= "bash: thiscmdshouldnotexist: command not found\n"
                     (:err e)))
              (is (= 127 (:exit e))))))

        (let [remote-path "/tmp/jepsen-upload-test"]
          (testing "upload"
            (c/exec :rm :-f remote-path)
            (c/upload-resource! test-resource remote-path)
            (is (= (slurp (io/resource test-resource))
                   (str (c/exec :cat remote-path) "\n"))))

          (testing "download"
            (let [tmp (File/createTempFile "jepsen-download" ".test")]
              (try
                (.delete tmp)
                (c/download remote-path (.getCanonicalPath tmp))
                (is (= (slurp (io/resource test-resource))
                       (slurp tmp)))
                (finally
                  (.delete tmp)))))

          (c/exec :rm :-f remote-path))))))

(deftest ^:integration ssh-remote-test
  (test-remote c/ssh))

(deftest ^:integration sshj-remote-test
  (test-remote (sshj/remote)))
