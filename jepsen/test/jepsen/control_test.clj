(ns jepsen.control-test
  (:require [clojure [string :as str]
                     [test :refer :all]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [common-test :refer [quiet-logging]]
                    [util :refer [contains-many? real-pmap]]]
            [jepsen.control.sshj :as sshj]
            [slingshot.slingshot :refer [try+ throw+]])
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
                (c/exec :echo "hello")
                (is false))
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

          (c/exec :rm :-f remote-path))

        (testing "thread safety"
          (let [; Which nodes are we going to act over?
                nodes ["n1" "n2" "n3" "n4" "n5"]
                ; Concurrency per node
                concurrency 10
                ; Makes the string we echo bigger or smaller
                str-count 10
                ; How many times do we run each command?
                rep-count 5
                ; What strings are we going to echo on each channel?
                xs  (->> (range concurrency)
                         (map (fn [i]
                                (str/join "," (repeat str-count i)))))
                t0 (System/nanoTime)]
            ; On each each node, and on concurrency channels, that string
            ; several times, making sure it comes back properly.
            (->> (for [node nodes, x xs]
                   (future
                     (dotimes [i rep-count]
                       ;(info :call node :i i :x x)
                       (is (= x (c/exec :echo x))))))
                 vec
                 (mapv deref))
            ;(info :time (-> (System/nanoTime) (- t0) (/ 1e6)) "ms")
            ))))))


(deftest ^:integration ssh-remote-test
  (info :clj-ssh)
  (test-remote c/ssh))

(deftest ^:integration sshj-remote-test
  (info :sshj)
  (test-remote (sshj/remote)))
