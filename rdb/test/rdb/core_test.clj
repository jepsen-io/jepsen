(ns rdb.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer :all]
            [clojure.java.io :as io]
            [rdb [core :as m]
                 [document-cas :as dc]]
            [jepsen [core      :as jepsen]
                    [util      :as util]
                    [checker   :as checker]
                    [model     :as model]
                    [tests     :as tests]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]]))

(defn run!
  [test]
  (let [test (jepsen/run! test)]
    (is (:valid? (:results test)))
    (report/to "report/history.edn"
               (pprint (:history test)))
    (report/to "report/linearizability.txt"
               (-> test :results :linear report/linearizability))))

(deftest document-safe-test (run! (dc/safe-test)))
