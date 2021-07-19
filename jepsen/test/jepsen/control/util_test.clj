(ns jepsen.control.util-test
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [string :as str]
                     [test :refer :all]]
            [clojure.java.io :as io]
            [jepsen.control :as c]
            [jepsen.control [util :as util]
                            [sshj :as sshj]]
            [slingshot.slingshot :refer [try+ throw+]]))

(use-fixtures :once (fn [t]
                      (c/with-ssh {}
                        (c/on "n1"
                          (t)))))

(defn assert-file-exists
  "Asserts that a file exists at a given destination"
  [dest file]
  (is (= (util/exists? (io/file dest file)) true)))

(defn assert-file-cached
  "Asserts that a file from a url was downloaded and cached in the wget-cache-dir"
  [url]
  (assert-file-exists util/wget-cache-dir (util/encode url)))

(deftest ^:integration daemon-test
  (let [logfile "/tmp/jepsen-daemon-test.log"
        pidfile "/tmp/jepsen-daemon-test.pid"]
    (c/exec :rm :-f logfile pidfile)
    (util/start-daemon! {:env {:DOG "bark"
                               :CAT "meow mix"}
                         :chdir "/tmp"
                         :logfile logfile
                         :pidfile pidfile}
                        "/usr/bin/perl"
                        :-e
                        "$|++; print \"$ENV{'CAT'}\\n\"; sleep 10;")
    (Thread/sleep 100)
    (let [pid   (str/trim (c/exec :cat pidfile))
          log   (c/exec :cat logfile)
          lines (str/split log #"\n")]
      (testing "pidfile exists"
        (is (re-find #"\d+" pid)))
      (testing "daemon running"
        (is (try+ (c/exec :kill :-0 pid)
                  true
                  (catch [:exit 1] _ false))))

      (testing "log starts with Jepsen debug line"
        (is (re-find #"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} Jepsen starting DOG=bark CAT=\"meow mix\" /usr/bin/perl -e \""
                     (first lines))))
      (testing "env vars threaded through to daemon"
        (is (= "meow mix" (nth lines 1))))

      (testing "shutdown"
        (util/stop-daemon! "/usr/bin/perl" pidfile)
        (testing "pidfile cleaned up"
          (is (not (util/exists? pidfile))))
        (testing "process exited"
          (is (is (try+ (c/exec :kill :-0 pid)
                        false
                        (catch [:exit 1] _ true)))))))))

(deftest ^:integration install-archive-test
  (testing "install-archive works without auth credentials"
    (let [dest "/tmp/test"
          url "https://storage.googleapis.com/etcd/v3.0.0/etcd-v3.0.0-linux-amd64.tar.gz"]
      (util/install-archive! (str url) dest {:force? true})
      (assert-file-exists dest "etcd")
      (assert-file-cached url)))

  (testing "install-archive works with auth credentials"
    (let [dest "/tmp/test"
          url "ftp://speedtest.tele2.net/1KB.zip"]
      (try+
        (util/install-archive! (str url) dest {:force? true :user? "anonymous" :pw? "anonymous"})
        (catch Object m
          ; The ZIP we download from the test server is not a real archive
          ; so unzip returns an exit code of 9 with a specific message 
          ; that must be in the exception if the download was successful
          (is (= (:exit m) 9))
          (.contains (:err m) "End-of-central-directory signature not found")
          (assert-file-cached url))))))

(deftest ^:integration cached-wget-test
  (testing "cached-wget works with and without auth credentials"
    (let [url "ftp://speedtest.tele2.net/1KB.zip"]
      (util/cached-wget! (str url) {:force? true})
      (assert-file-cached url)
      (util/cached-wget! (str url) {:force? true :user? "anonymous" :pw? "anonymous"})
      (assert-file-cached url))))
