(ns jepsen.robustirc-test
  (:require [clojure.test     :refer :all]
            [jepsen.core      :refer [run!]]
            [jepsen.robustirc :refer :all]))

(def version "0.1")

(deftest sets-test'
  (is (:valid? (:results (run! (sets-test version))))))
