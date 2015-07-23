(ns jepsen.mongodb-rocks-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.mongodb-rocks :refer :all]))

(deftest logger-perf
  ; WiredTiger crashes, woooo
;  (is (:valid? (:results (run! (logger-perf-test "3.0.4~pre" "wiredTiger")))))
  (is (:valid? (:results (run! (logger-perf-test "3.0.4~pre" "mmapv1")))))
  (is (:valid? (:results (run! (logger-perf-test "3.0.4~pre" "rocksdb")))))
  )
