(ns jepsen.rethinkdb-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer :all]
            [clojure.java.io :as io]
            [jepsen.rethinkdb.document-cas :as dc]
            [jepsen.core :as jepsen]))

(defn run!
  [test]
  (let [test (jepsen/run! test)]
    (is (:valid? (:results test)))))

(deftest document-safe-test
  (run! (dc/cas-test "2.1.5+2~0jessie" "single" "majority")))
;(deftest document-unsafe-test
;  (run! (dc/cas-test "2.1.5+2~0jessie" "majority" "majority")))
