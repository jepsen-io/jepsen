(ns jepsen.percona-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.percona :refer :all]
            [jepsen.percona.dirty-reads :as dirty-reads]))

(def version "5.6.25-25.12-1.jessie")

;(deftest sets-test'
;  (is (:valid? (:results (run! (sets-test version))))))

(deftest bank-test'
  (is (:valid? (:results (run! (bank-test version 2 10 " FOR UPDATE" false))))))

;(deftest dirty-reads-test
;  (is (:valid? (:results (run! (dirty-reads/test- version 4))))))
