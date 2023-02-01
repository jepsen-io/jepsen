(ns jepsen.repl
  "Helper functions for mucking around with tests!"
  (:require [jepsen [history :as h]
                    [report :as report]
                    [store :as store]]))

(defn latest-test
  "Returns the most recently run test"
  []
  (store/latest))
