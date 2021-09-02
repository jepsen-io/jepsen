(ns jepsen.tests.cycle
  "Tests based on transactional cycle detection via Elle. If you're looking for
  code that used to be here, see elle.core."
  (:require [jepsen [checker :as checker]
                    [store :as store]]
            [elle.core :as elle]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn checker
  "Takes a function which takes a history and returns a [graph, explainer]
  pair, and returns a checker which uses those graphs to identify cyclic
  dependencies."
  [analyze-fn]
  (reify checker/Checker
    (check [this test history opts]
      (elle/check {:analyzer analyze-fn} history))))
