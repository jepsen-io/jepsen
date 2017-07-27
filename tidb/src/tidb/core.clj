(ns tidb.core
  (:gen-class)
  (:require [jepsen
              [generator :as gen]
              [cli :as cli]
              [checker :as checker]
            ]
            [tidb.bank :as bank]
  )
)

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn bank/bank-test}) args)
)
