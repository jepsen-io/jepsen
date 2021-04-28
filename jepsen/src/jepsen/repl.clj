(ns jepsen.repl
  "Helper functions for mucking around with tests!"
  (:require [jepsen.store :as store]
            [jepsen.report :as report]))

(defn latest-test
  "Returns the most recently run test"
  []
  (store/latest))
