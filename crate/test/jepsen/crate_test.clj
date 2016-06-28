(ns jepsen.crate-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.crate :refer :all]))

(deftest a-test
  (jepsen/run! (an-test {})))
