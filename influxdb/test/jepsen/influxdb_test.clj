(ns jepsen.influxdb-test
  (:require [clojure.test :refer :all]
  	 		[jepsen.core :refer [run!]]
            [jepsen.influxdb :as influxdb]))


(def version
  "What influxdb version should we test?"
  "0.9.6.1")


(deftest basic-test
  (is (:valid? (:results (run! (influxdb/basic-test version))))))

