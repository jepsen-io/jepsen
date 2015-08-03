(ns jepsen.chronos-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.chronos :refer :all]))

(deftest install-test
  (is (:valid? (:results (run! (simple-test "0.23.0-1.0.debian81"
                                            "2.3.4-1.0.81.debian77"))))))
