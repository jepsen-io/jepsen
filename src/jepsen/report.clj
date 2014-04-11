(ns jepsen.report
  "Prints out stuff."
  (:require [jepsen.util :as util]
            [clojure.pprint :refer [pprint]]))

(defn linearizability
  "Print out information about the linearizability results from Knossos"
  [res]
  (when-not (:valid? res)
    (println)
    (println "Not linearizable. Linearizable prefix was:")
    (->> res :linearizable-prefix util/print-history)

    (println)
    (println "Followed by inconsistent operation:")
    (println (util/op->str (:inconsistent-op res)))

    (println)
    (println "Last consistent worlds were: ----------------")
    (doseq [world (:last-consistent-worlds res)]
      (println "World with fixed history:")
      (util/print-history (:fixed world))
      (println "led to current state: ")
      (pprint (:model world))
      (println "with pending operations:")
      (util/print-history (:pending world))
      (println))
    (println "---------------------------------------------")

    (println)
    (println "Inconsistent state transitions:")
    (pprint (:inconsistent-transitions res))))
