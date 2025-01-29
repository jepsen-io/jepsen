(ns jepsen.disque-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [jepsen [core :as jepsen]
                    [disque :as disque]
                    [report :as report]]))

(deftest partitions
  (let [test (jepsen/run! (disque/partitions-test))]
    (is (:valid? (:results test)))))
