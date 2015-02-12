(ns jepsen.report
  "Prints out stuff."
  (:require [jepsen.util :as util]
            [clojure.pprint :refer [pprint]]))

(defn linearizability
  "Print out information about the linearizability results from Knossos"
  [res]
  (if (:valid? res)
    (println (count (:linearizable-prefix res))
             "element history linearizable. :D")
    (do
      (println "Not linearizable. Linearizable prefix was:")
      (->> res :linearizable-prefix util/print-history)

      (println)
      (println "Followed by inconsistent operation:")
      (println (util/op->str (:inconsistent-op res)))

      (println)
      (println "-------------------------------------------------------------")
      (println "Just prior to that operation, possible interpretations of the")
      (println "linearizable prefix were:")
      (doseq [world (take 32 (shuffle (:last-consistent-worlds res)))]
        (println "World with fixed history:")
        (util/print-history (:fixed world))
        (println)
        (println "led to state: ")
        (pprint (:model world))
        (println)
        (println "with pending operations:")
        (util/print-history (:pending world))
        (println))
      (let [c (count (:last-consistent-worlds res))]
        (when (< 32 c)
          (println "(and" (- c 32) "more worlds, elided here)")))
      (println "--------------------------------------------------------------")

      (println)
      (println "Inconsistent state transitions:")
      (pprint (distinct (:inconsistent-transitions res))))))
