(ns jepsen.repl
  "Helper functions for mucking around with tests!"
  (:require [jepsen.store :as store]
            [jepsen.report :as report]))

(defn last-test
  "Returns the most recently run test"
  [test-name]
  (->> (store/tests test-name)
       (sort-by key)
       last
       val
       deref))
