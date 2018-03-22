(ns jepsen.fauna
  (:require [clj-yaml.core :as yaml]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [client :as client]
             [control :as c :refer [|]]
             [core :as jepsen]
             [db :as db]
             [generator :as gen]
             [tests :as tests]]
            [jepsen.faunadb.client :as f]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def root-key
  "Administrative key for the FaunaDB cluster."
  "secret")

(def repo-key
  "FaunaDB dpkg repository key."
  "TPwTIfv9rYCBsY9PR2Y31F1X5JEUFIifWopdM3RvdHXaLgjkOl0wPoNp1kif1hJS")

(def partitions
  "The number of log partitions in the FaunaDB cluster."
  3)

(defn install!
  "Install a particular version of FaunaDB."
  [version]
  (debian/install-jdk8!)
  (debian/add-repo! "faunadb"
                    (str/join ["deb [arch=all] https://" repo-key "@repo.fauna.com/enterprise/debian unstable non-free"]))
  (c/su (c/exec :wget :-qO :- "https://repo.fauna.com/faunadb-gpg-public.key" |
                :apt-key :add :-))
  (debian/install {"faunadb" version}))

(defn log-configuration
  [test node]
  (vals
   (group-by (fn [n]
               (mod (+ 1 (.indexOf (:nodes test) n)) partitions))
             (:nodes test))))

(defn configure!
  "Configure FaunaDB."
  [test node]
  (info "Configuring" node)
  (c/su
   (c/exec :echo (-> "faunadb.conf"
                     io/resource
                     slurp)
           :> "/etc/init/faunadb.conf")
   (c/exec :echo
           (yaml/generate-string
            (merge
             (yaml/parse-string (-> "faunadb.yml"
                                    io/resource
                                    slurp))
             {:auth_root_key root-key
              :network_coordinator_http_address node
              :network_broadcast_address node
              :network_datacenter_name "replica-1"
              :network_host_id node
              :network_listen_address node
              :storage_transaction_log_nodes (log-configuration test node)}))
            :> "/etc/faunadb.yml")))

(defn db
  "FaunaDB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (install! version)
      (configure! test node)
      (c/su
       (c/exec :initctl :start :faunadb)
       (Thread/sleep 30000)
       (jepsen/synchronize test)

       (when (= node (jepsen/primary test))
         (info node "initializing FaunaDB cluster")
         (c/exec :faunadb-admin :init)
         (Thread/sleep 10000)))
      (jepsen/synchronize test)

      (when (not= node (jepsen/primary test))
        (info node "joining FaunaDB cluster")
        (c/exec :faunadb-admin :join (jepsen/primary test))
        (Thread/sleep 10000))
      (jepsen/synchronize test))


    (teardown! [_ test node]
      (info node "tearing down FaunaDB")
      (c/su
       (c/exec :initctl :stop :faunadb)
       (debian/uninstall! :faunadb)
       (c/exec :rm :-rf "/var/lib/faunadb")))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/faunadb/core.log"])))

(defn basic-test
  "Sets up the test parameters common to all tests."
  [opts]
  (merge
    tests/noop-test
    {:name    (str "faunadb" (str ":" (:name (:nemesis opts))))
     :os      debian/os ;; NB. requires Ubuntu 14.04 LTS
     :db      (db "2.5.0-0")
     :client  (:client (:client opts))
     :nemesis (:client (:nemesis opts))
     :generator (gen/phases
                  (->> (gen/nemesis (:during (:nemesis opts))
                                    (:during (:client opts)))
                       (gen/time-limit (:time-limit opts)))
                  (gen/log "Nemesis terminating")
                  (gen/nemesis (:final (:nemesis opts)))
                  ; Final client
                  (gen/clients (:final (:client opts))))}
    (dissoc opts :name :client :nemesis)))
