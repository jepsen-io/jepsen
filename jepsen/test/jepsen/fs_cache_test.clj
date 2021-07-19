(ns jepsen.fs-cache-test
  (:require [clojure [test :refer :all]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [common-test :refer [quiet-logging]]
                    [fs-cache :as cache]]
            [jepsen.control [sshj :as sshj]
                            [util :as cu]]))

(use-fixtures :once quiet-logging)
(use-fixtures :each (fn [t]
                      (cache/clear!)
                      (t)
                      (cache/clear!)))

(deftest fs-path-test
  (is (= ["dk_dog"
          "ds_meow\\/woof\\\\evil"
          "dl_3"
          "db_false"
          "db_true"
          "dm_11"
          "fn_12"]
         (cache/fs-path [:cat/dog
                         "meow/woof\\evil"
                         3
                         false
                         true
                         11M
                         12N]))))

(deftest file-test
  (testing "empty"
    (is (thrown? IllegalArgumentException (cache/file []))))

  (testing "single path"
    (is (= (io/file "/tmp/jepsen/cache/fk_foo")
           (cache/file [:foo]))))

  (testing "multiple paths"
    (is (= (io/file "/tmp/jepsen/cache/dk_foo/fs_bar")
           (cache/file [:foo "bar"])))))

(deftest file-test
  (let [f         (io/file "/tmp/jepsen/cache-test")
        contents  "hello there"
        path      [:foo]]
    (spit f contents)

    (testing "not cached"
      (is (not (cache/cached? path)))
      (is (nil? (cache/load-file path))))

    (testing "cached"
      (cache/save-file! f path)
      (is (cache/cached? path)))

    (testing "read as file"
      (is (= contents (slurp (cache/load-file path)))))

    (testing "read as string"
      (is (= contents (cache/load-string path))))))

(deftest string-test
  (let [contents "foo\nbar"
        path [1 2 3]]
    (testing "not cached"
      (is (not (cache/cached? path)))
      (is (nil? (cache/load-string path))))

    (testing "cached"
      (cache/save-string! contents path)
      (is (cache/cached? path))
      (is (= contents (cache/load-string path))))))

(deftest edn-test
  (let [contents {:foo [1 2N "three" 'four]}
        path     ["weirdly" :specific true]]
    (testing "not cached"
      (is (not (cache/cached? path)))
      (is (nil? (cache/load-edn path))))

    (testing "cached"
      (cache/save-edn! contents path)
      (is (cache/cached? path))
      (is (= contents (cache/load-edn path))))))

(defmacro on-n1
  "Run form with an SSH connection to n1"
  [& body]
  ; Right now sshj is faster but not yet the default, so we
  ; set it explicitly.
  `(c/with-remote (sshj/remote)
    (c/with-ssh {}
      (c/on "n1"
            ~@body))))

(deftest ^:integration remote-test
  (on-n1
    (let [contents "foo\nbar"
          local-path  [:local]
          remote-path "/tmp/jepsen/remote"]
      (c/exec :rm :-rf "/tmp/jepsen")

      (testing "upload"
        (cache/save-string! contents local-path)
        (cache/deploy-remote! local-path remote-path)
        (is (= contents (c/exec :cat remote-path))))

      (testing "download"
        (cache/clear! local-path)
        (is (not (cache/cached? local-path)))
        (cache/save-remote! remote-path local-path)
        (is (cache/cached? local-path))
        (is (= contents (cache/load-string local-path)))))))
