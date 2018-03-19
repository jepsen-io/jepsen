(ns jepsen.fauna
  (:import com.faunadb.client.FaunaClient)
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [client :as client]
             [control :as c :refer [|]]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [clj-yaml.core :as yaml]))

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
      (info node "installing FaunaDB" version)
      (install! version)
      (configure! test node))

    (teardown! [_ test node]
      (info node "tearing down FaunaDB")
      (c/su
       ;; this over-complicated pipeline checks upstart for a
       ;; configured _and_ running faunadb instance, before attempting
       ;; to kill it.
       (c/exec :initctl :list |
               :grep :faunadb |
               :xargs :-r |
               :grep "start" |
               :xargs :-r |
               :cut :-f1 :-d\' \' |
               :xargs :-r :initctl :stop)
       (c/exec :rm :-rf "/var/lib/faunadb")))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/faunadb/core.log"])))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (doto (FaunaClient/builder)
                        (.withSecret root-key)
                        (.build))))

  (setup! [this test])

  (invoke! [_ test op])

  (teardown! [this test])

  (close! [_ test]))

(defn fauna-test
  "Given an options map from the command line
  runner (e.g. :nodes, :ssh, :concurrency, ...), constructs a test
  map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "faunadb"
          :os    debian/os ;; NB. requires Ubuntu 14.04 LTS
          :db    (db "2.5.0-0")
          :client (Client. nil)}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing result."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn fauna-test})
                   (cli/serve-cmd))
            args))
