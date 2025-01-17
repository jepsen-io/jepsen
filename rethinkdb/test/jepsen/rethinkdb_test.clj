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

(let [; version "2.1.5+2~0jessie"
      version "2.2.3+1~0jessie"]
  (deftest single-single-test
    (run! (dc/cas-test version "single" "single")))
  (deftest majority-single-test
    (run! (dc/cas-test version "majority" "single")))
  (deftest single-majority-test
    (run! (dc/cas-test version "single" "majority")))
  (deftest majority-majority-test
    (run! (dc/cas-test version "majority" "majority")))

  (deftest reconfigure-test
    (run! (dc/cas-reconfigure-test version))))
