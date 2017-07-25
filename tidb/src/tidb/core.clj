(ns tidb.core
  (:gen-class)
  (:require [jepsen
              [tests :as tests]
              [generator :as gen]
              [cli :as cli]
            ]
            [jepsen.os.debian :as debian]
            [tidb.db :as db]
  )
)

(defn tidb-test
  [opts]
    (merge tests/noop-test
      {:name "TiDB"
       :os debian/os
       :db (db/db opts)
      }
      opts
    )
)

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn bank-test}) args)
)
