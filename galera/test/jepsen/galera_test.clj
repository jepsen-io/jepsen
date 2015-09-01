(ns jepsen.galera-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.galera :refer :all]))

(def version "7.4.7")

;(deftest sets-test'
;  (is (:valid? (:results (run! (sets-test version))))))

(deftest bank-test'
  (is (:valid? (:results (run! (bank-test version 2 10))))))
