(ns tidb.basic
  (:require [clojure.string :as str]
            [jepsen
              [tests :as tests]
              [generator :as gen]
            ]
            [jepsen.os.debian :as debian]
            [tidb.db :as db]
  )
)

(defn basic-test
  "Sets up the test parameters common to all tests."
  [opts]
  (let [gen (->> (:during (:client opts))
                 (gen/nemesis (:during (:nemesis opts)))
                 (gen/time-limit (:time-limit opts)))
        gen (if (:final (:client opts))
              (gen/phases gen
                          (gen/log "Nemesis terminating")
                          (gen/nemesis (:final (:nemesis opts)))
                          (gen/log "Waiting for quiescence")
                          (gen/sleep (:recovery-time opts))
                          (gen/clients (:final (:client opts))))
              gen)]
  (merge
    tests/noop-test
    {:os      debian/os
     :concurrency 20
     :name    (str "TiDB-" (:name opts))
     :db      (db/db opts)
     :client  (:client (:client opts))
     :nemesis (:client (:nemesis opts))
     :generator gen
     :keyrange (atom {})}
    (dissoc opts :name :client :nemesis))))
