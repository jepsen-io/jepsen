(ns tidb.core
  (:gen-class)
  (:require [jepsen
              [tests :as tests]
              [cli :as cli]
            ]
            [tidb.db :as db]
  )
)

(defn tidb-test
  [opts]
    (merge tests/noop-test
      {:name "TiDB"
       :db (db/db opts)
      }
      opts
    )
)

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn tidb-test}) args)
)
