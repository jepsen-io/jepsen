(ns jepsen.cockroach
  "Tests for CockroachDB"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.jdbc :as j]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen
             [core :as jepsen]
             [db :as db]
             [os :as os]
             [tests :as tests]
             [control :as c :refer [|]]
             [store :as store]
             [nemesis :as nemesis]
             [generator :as gen]
             [independent :as independent]
             [reconnect :as rc]
             [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.cockroach [nemesis :as cln]
                              [auto :as auto :refer [cockroach-user
                                                     cockroach
                                                     jdbc-mode
                                                     db-port
                                                     insecure
                                                     db-user
                                                     db-passwd
                                                     store-path
                                                     dbname
                                                     verlog
                                                     log-files
                                                     pcaplog]]]))

(import [java.net URLEncoder])

;; number of simultaneous clients
(def concurrency-factor 30)

(defn db
  "Sets up and tears down CockroachDB."
  [opts]
  (reify db/DB
    (setup! [_ test node]
      (when (= node (jepsen/primary test))
        (store/with-out-file test "jepsen-version.txt"
          (meh (->> (sh "git" "describe" "--tags")
                    (:out)
                    (print)))))

      (when (= jdbc-mode :cdb-cluster)
        (auto/install! test node)
        (auto/reset-clock!)
        (jepsen/synchronize test)

        (c/sudo cockroach-user
                (when (= node (jepsen/primary test))
                  (auto/start! test node)
                  (Thread/sleep 10000))

                (jepsen/synchronize test)
                (auto/packet-capture! node)
                (auto/save-version! node)
                (when (not= node (jepsen/primary test))
                  (auto/join! test node))

                (jepsen/synchronize test)
                (when (= node (jepsen/primary test))
                  (Thread/sleep 2000)
                  (auto/set-replication-zone!  ".default"
                                              {:range_min_bytes 1024
                                               :range_max_bytes 1048576})
                  (info node "Creating database...")
                  (auto/csql! (str "create database " dbname))))

        (info node "Setup complete")))

    (teardown! [_ test node]
      (when (= jdbc-mode :cdb-cluster)
        (auto/reset-clock!)

        (c/su
          (auto/kill! test node)

          (info node "Erasing the store...")
          (c/exec :rm :-rf store-path)

          (info node "Stopping tcpdump...")
          (meh (c/exec :killall -9 :tcpdump))

          (info node "Clearing the logs...")
          (doseq [f log-files]
            (when (cu/exists? f)
              (c/exec :truncate :-c :--size 0 f)
              (c/exec :chown cockroach-user f))))))

    db/LogFiles
    (log-files [_ test node] log-files)))

(defn covering-range
  "Takes a pair of [low, high] and an element x, and expands the range to cover
  it."
  [[low high :as r] x]
  (cond
    (nil? r)   [x x]
    (< x low)  [x high]
    (< high x) [low x]
    true       r))

(defn update-keyrange!
  "A keyrange is used to track which keys a test is using, so we can split
  them. This function takes a test and updates its :keyrange atom to include
  the given table and key."
  [test table k]
  (if-let [r (:keyrange test)]
    (swap! r update table covering-range k)
    (throw (IllegalArgumentException. "No :keyrange in test"))))

;;;;;;;;;;;;;;;;;;;;;;;; Common test definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn str->int [str]
  (let [n (read-string str)]
    (if (number? n) n nil)))

(defn basic-test
  "Sets up the test parameters common to all tests."
  [opts]
  (merge
    tests/noop-test
    {:nodes   (if (= jdbc-mode :cdb-cluster) (:nodes opts) [:localhost])
     :name    (str "cockroachdb-" (:name opts)
                   (if (:linearizable opts) "-lin" "")
                   (if (= jdbc-mode :cdb-cluster)
                     (str ":" (:name (:nemesis opts)))
                     "-fake"))
     :db      (db opts)
     :os      (if (= jdbc-mode :cdb-cluster) ubuntu/os os/noop)
     :client  (:client (:client opts))
     :nemesis (if (= jdbc-mode :cdb-cluster)
                (:client (:nemesis opts))
                nemesis/noop)
     :generator (gen/phases
                  (->> (gen/nemesis (:during (:nemesis opts))
                                    (:during (:client opts)))
                       (gen/time-limit (:time-limit opts)))
                  (gen/log "Nemesis terminating")
                  (gen/nemesis (:final (:nemesis opts)))
                  (gen/log "Waiting for quiescence")
                  (gen/sleep (:recovery-time opts))
                  ; Final client
                  (gen/clients (:final (:client opts))))}
    (dissoc opts :name :nodes :client :nemesis)))
