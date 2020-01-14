(ns jepsen.control.util-test
  (:require [jepsen.control :as c]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen.control.util :as util]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(defn assert-file-exists
  "Asserts that a file exists at a given destination"
  [dest file]
  (is (= (util/exists? (io/file dest file)) true)))

(defn assert-file-cached
  "Asserts that a file from a url was downloaded and cached in the wget-cache-dir"
  [url]
  (assert-file-exists util/wget-cache-dir (util/encode url)))

(deftest ^:integration install-archive-test
  (testing "install-archive works without auth credentials"
    (c/with-ssh {}
      (c/on "n1"
        (let [dest "/tmp/test"
              url "https://storage.googleapis.com/etcd/v3.0.0/etcd-v3.0.0-linux-amd64.tar.gz"]
          (util/install-archive! (str url) dest {:force? true})
          (assert-file-exists dest "etcd")
          (assert-file-cached url)))))
  
  (testing "install-archive works with auth credentials"
    (let [dest "/tmp/test"
          url "ftp://speedtest.tele2.net/1KB.zip"]
      (c/with-ssh {}
        (c/on "n1"
          (try+
            (util/install-archive! (str url) dest {:force? true :user? "anonymous" :pw? "anonymous"})
          (catch Object m
            ; The ZIP we download from the test server is not a real archive
            ; so unzip returns an exit code of 9 with a specific message 
            ; that must be in the exception if the download was successful
            (is (= (:exit m) 9))
            (.contains (:err m) "End-of-central-directory signature not found")
            (assert-file-cached url))))))))

(deftest ^:integration cached-wget-test
  (testing "cached-wget works with and without auth credentials"
    (c/with-ssh {}
      (c/on "n1"
        (let [url "ftp://speedtest.tele2.net/1KB.zip"]
          (util/cached-wget! (str url) {:force? true})
          (assert-file-cached url)
          (util/cached-wget! (str url) {:force? true :user? "anonymous" :pw? "anonymous"})
          (assert-file-cached url))))))
