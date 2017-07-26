(ns tidb.core
  (:gen-class)
  (:require [jepsen
              [tests :as tests]
              [generator :as gen]
              [cli :as cli]
            ]
            [jepsen.os.debian :as debian]
            [tidb.db :as db]
            [tidb.client :as client]
  )
)

(def n 2)
(def initial-balance 10)

(defn bank-test
  [opts]
  (merge tests/noop-test
    {:os debian/os
     :name "TiDB-Bank"
     :concurrency 20
     :model  {:n n :total (* n initial-balance)}
     :db (db/db opts)
     :client (client/bank-client n initial-balance " FOR UPDATE" false)
     :generator (gen/phases
                  (->> (gen/mix [client/bank-read client/bank-diff-transfer])
                       (gen/clients)
                       (gen/stagger 1/10)
                       (gen/time-limit 60))
                  (gen/log "waiting for quiescence")
                  (gen/sleep 10)
                  (gen/clients (gen/once client/bank-read)))
    }
  )
)

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn bank-test}) args)
)
