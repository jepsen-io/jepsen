(ns jepsen.control-test
  (:require [clojure [string :as str]
                     [test :refer :all]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [common-test :refer [quiet-logging]]
                    [util :refer [contains-many? real-pmap]]]
            [jepsen.control [sshj    :as sshj]
                            [clj-ssh :as clj-ssh]
                            [util    :as cu]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io File)))

(use-fixtures :once quiet-logging)

(def test-resource
  "A file in /resources that we can use to verify uploads/downloads."
  "bump-time.c")

(defn test-remote
  [remote]
  (c/with-ssh {}
    (c/with-remote remote
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
              (is (= "bash: line 1: thiscmdshouldnotexist: command not found\n"
                     (:err e)))
              (is (= 127 (:exit e))))))

        (let [remote-path "/tmp/jepsen-upload-test"
              contents (slurp (io/resource test-resource))]
          (testing "upload"
            (c/exec :rm :-f remote-path)
            (c/upload-resource! test-resource remote-path)
            (is (= contents (str (c/exec :cat remote-path) "\n"))))

          (testing "upload as different user"
            (c/exec :rm :-f remote-path)
            (c/sudo "nobody"
                    (c/upload-resource! test-resource remote-path)
                    (is (= contents (str (c/exec :cat remote-path) "\n")))
                    (is (= "nobody" (c/exec :stat :-c "%U" remote-path)))))

          (testing "download"
            (let [tmp (File/createTempFile "jepsen-download" ".test")]
              (try
                (.delete tmp)
                (c/download remote-path (.getCanonicalPath tmp))
                (is (= contents (slurp tmp)))
                (finally
                  (.delete tmp)))))

          (testing "download as different user"
            (let [tmp (File/createTempFile "jepsen-download" ".test")]
              (try
                (.delete tmp)
                ; What we should REALLY do here is show that we can log in as a
                ; passwordless sudo user without privileges to read a file,
                ; then download it anyway, but... that'd require extra setup
                ; for the test env that I don't want to do yet.
                (c/sudo "nobody"
                        (c/download remote-path (.getCanonicalPath tmp)))
                (is (= contents (slurp tmp)))
                (finally
                  (.delete tmp)))))

          (c/exec :rm :-f remote-path))

        (testing "very basic stdin"
          (let [string "hi"
                res (-> {:cmd "cat", :in string}
                        c/ssh*
                        c/throw-on-nonzero-exit
                        :out)]
            (is (= string res))))

        (testing "write file as different user"
          ; This uses stdin to write the file contents, and wrapping it in a
          ; sudo requires that we *not* provide a password for sudo here, lest
          ; it be passed in to cat and show up in the file.
          (c/su "nobody"
                (let [tmp    (cu/tmp-file!)
                      string "bark\narf"]
                  (try
                    (cu/write-file! string tmp)
                    (is (= string (c/exec :cat tmp)))
                    (finally
                      (c/exec :rm :-f tmp))))))

        ;(testing "writing to file"
        ;  (let [file "/tmp/jepsen/test/write-file-test"
        ;        string "foo\nbar"]
        ;    (c/exec :mkdir :-p "/tmp/jepsen/test")
        ;    (cu/write-file! string file)
        ;    (is (= string (c/exec :cat file)))))

        (testing "thread safety"
          (let [; Concurrency
                concurrency 20
                ; Makes the string we echo bigger or smaller
                str-count 10
                ; How many times do we run each command?
                rep-count 5
                ; What strings are we going to echo on each channel?
                xs  (->> (range concurrency)
                         (map (fn [i]
                                (str/join "," (repeat str-count i)))))
                t0 (System/nanoTime)]
            ; On concurrency channels, echo that string several times, making
            ; sure it comes back properly.
            (->> (for [x xs]
                   (future
                     (dotimes [i rep-count]
                       ;(info :call node :i i :x x)
                       (is (= x (c/exec :echo x))))))
                 vec
                 (mapv deref))
            ;(info :time (-> (System/nanoTime) (- t0) (/ 1e6)) "ms")
            ))

        (testing "handles interrupts correctly"
          (let [thread (Thread/currentThread)
                ; Interrupt ourselves during exec
                killer (future
                         (Thread/sleep 20)
                         (.interrupt thread))
                res (try (c/exec :sleep 1)
                         :unreachable
                         (catch InterruptedException ee
                           :interrupted)
                         (catch java.io.InterruptedIOException e
                           :interrupted-io))]
            (is #{:interrupted :interrupted-io} res)))))))

(deftest ^:integration clj-ssh-remote-test
  ;(info :clj-ssh)
  (test-remote (clj-ssh/remote)))

(deftest ^:integration sshj-remote-test
  ;(info :sshj)
  (test-remote (sshj/remote)))
