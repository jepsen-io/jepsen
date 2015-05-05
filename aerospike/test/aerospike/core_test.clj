(ns aerospike.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [aerospike.core :refer :all]
            [jepsen [core :as jepsen]
                    [report :as report]]))

(deftest cas-register
  (let [test (jepsen/run! (cas-register-test))]
    (is (:valid? (:results test)))
    (report/to "report/history.edn" (pprint (:history test)))
    (report/to "report/linearizability.txt"
               (-> test :results :linear report/linearizability))))

(deftest counter
  (let [test (jepsen/run! (counter-test))]
    (is (:valid? (:results test)))
    (report/to "report/counter.txt"
               (-> test :results :counter pprint))))
