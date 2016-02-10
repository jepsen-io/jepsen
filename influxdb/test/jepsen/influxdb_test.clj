(ns jepsen.influxdb-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.control :as control]
            [jepsen.influxdb :as influxdb])
  (:import (org.influxdb InfluxDB$ConsistencyLevel)))

(defn run!
  [test]
  (let [test (jepsen/run! test)]
    (is (:valid? (:results test)))))

(deftest healthy-single-shard-data-rw-consistency-all
  (run! (influxdb/test-on-single-shard-data "0.10.0-1" "healthy-sp" InfluxDB$ConsistencyLevel/ALL true)))

(deftest healthy-single-shard-data-rw-consistency-any
  (run! (influxdb/test-on-single-shard-data "0.10.0-1" "healthy-sp" InfluxDB$ConsistencyLevel/ANY true)))

(deftest single-shard-data-rw-consistency-all
  (run! (influxdb/test-on-single-shard-data "0.10.0-1" "nemesis-single-point" InfluxDB$ConsistencyLevel/ALL false)))

(deftest single-shard-data-rw-consistency-any
  (run! (influxdb/test-on-single-shard-data "0.10.0-1" "nemesis-single-point" InfluxDB$ConsistencyLevel/ANY false)))
