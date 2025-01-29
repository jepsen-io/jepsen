(ns aerospike.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [aerospike.core :refer :all]
            [jepsen [core :as jepsen]
                    [report :as report]]))

(deftest cas-register
  (let [test (jepsen/run! (cas-register-test))]
    (is (:valid? (:results test)))))

;(deftest counter
;  (let [test (jepsen/run! (counter-test))]
;    (is (:valid? (:results test)))))
