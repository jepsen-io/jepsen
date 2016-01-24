(ns jepsen.influxdb-test
  (:require [clojure.test :refer :all]
  	 		[jepsen.core :as jepsen]
            [jepsen.influxdb :as influxdb]))

(defn run!
  [test]
  (let [test (jepsen/run! test)]
    (is (:valid? (:results test)))))

(deftest single-shard-data-rw-consistency-all
  (is (:valid? (:results (jepsen/run! (influxdb/test-on-single-shard-data "0.9.6.1" "single-point-cons-ALL"))))))

