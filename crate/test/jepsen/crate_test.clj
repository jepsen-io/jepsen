(ns jepsen.crate-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.crate :refer :all]
            [jepsen.crate.lost-updates :as lost-updates]))

;(deftest a-test
;  (jepsen/run! (an-test {})))

(deftest lost-updates-test
  (jepsen/run! (lost-updates/test {})))
