(ns jepsen.fauna
  (:require
            [clojure.tools.logging :refer :all]
            [jepsen [cli :as cli]
             [core :as jepsen]
             [db :as db]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.os.debian :as debian]
            [jepsen.faunadb.auto :as auto]))

(def replicas
  "The number of replicas in the FaunaDB cluster."
  3)

(defn db
  "FaunaDB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (let [width (min replicas (count (:nodes test)))]
        (auto/install! version)
        (auto/configure! test node width)
        (auto/start! test node width)))

    (teardown! [_ test node]
      (info node "tearing down FaunaDB")
      (auto/teardown! test node))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/faunadb/core.log"
       "/var/log/faunadb/query.log"
       "/var/log/faunadb/exception.log"])))

(defn basic-test
  "Sets up the test parameters common to all tests."
  [opts]
  (merge
    tests/noop-test
    {:name    (str "faunadb"
                   (str ":" (:name opts))
                   (str ":" (:name (:nemesis opts))))
     :os      debian/os ;; NB. requires Ubuntu 14.04 LTS
     :db      (db "2.5.1.rc1-0")
     :client  (:client (:client opts))
     :nemesis (:nemesis (:nemesis opts))
     :generator (gen/phases
                  (->> (gen/nemesis (:during (:nemesis opts))
                                    (:during (:client opts)))
                       (gen/time-limit (:time-limit opts)))
                  (gen/log "Nemesis terminating")
                  (gen/nemesis (:final (:nemesis opts)))
                  ; Final client
                  (gen/clients (:final (:client opts))))}
    (dissoc opts :name :client :nemesis)))
