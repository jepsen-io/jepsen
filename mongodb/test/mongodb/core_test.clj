(ns mongodb.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer :all]
            [clojure.java.io :as io]
            [mongodb.core :as m]
            [jepsen [core      :as jepsen]
                    [util      :as util]
                    [checker   :as checker]
                    [model     :as model]
                    [tests     :as tests]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]]))

(deftest document-cas-test
  (let [test (jepsen/run! (m/document-cas-no-read-majority-test))]
    (is (:valid? (:results test)))
    (report/to "report/history.edn"
               (pprint (:history test)))
    (report/to "report/linearizability.txt"
               (-> test :results :linear report/linearizability))))
