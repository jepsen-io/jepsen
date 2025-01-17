(ns jepsen.postgres-rds-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.postgres-rds :refer [bank-test]]))

(def node "jepsen.ciudayaehbts.us-west-2.rds.amazonaws.com")

(deftest bank-test'
  (is (:valid? (:results (jepsen/run! (bank-test
                                        node
                                        2
                                        10
                                        ""
                                        false))))))
