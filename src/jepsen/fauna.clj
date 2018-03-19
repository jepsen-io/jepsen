(ns jepsen.fauna
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [control :as c :refer [|]]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [yaml.core :as yaml]))

(def root-key
  "Administrative key for the FaunaDB cluster."
  "secret")

(def repo-key
  "FaunaDB dpkg repository key."
  "TPwTIfv9rYCBsY9PR2Y31F1X5JEUFIifWopdM3RvdHXaLgjkOl0wPoNp1kif1hJS")

(defn install!
  "Install a particular version of FaunaDB."
  [version]
  (debian/install-jdk8!)
  (debian/add-repo! "faunadb"
                    (str/join ["deb [arch=all] https://" repo-key "@repo.fauna.com/enterprise/debian unstable non-free"]))
  (c/su (c/exec :wget :-qO :- "https://repo.fauna.com/faunadb-gpg-public.key" |
                :apt-key :add :-))
  (debian/install {"faunadb" version}))

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
            (merge (yaml/parse-string
                    (-> "faunadb.yml"
                        io/resource
                        slurp))
                   {:auth_root_key root-key
                    :network_broadcast_address node
                    :network_host_id node
                    :network_listen_address node
                    :storage_transaction_log_nodes [[node]]}))
            :> "/etc/faunadb.yml")))

(defn db
  "FaunaDB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing FaunaDB" version)
      (install! version)
      (configure! test node)
      (c/su (c/exec :initctl :start :faunadb)))

    (teardown! [_ test node]
      (info node "tearing down FaunaDB")
      (c/su
       (c/exec :initctl :stop :faunadb)
       (c/exec :rm :-rf "/var/lib/faunadb")))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/faunadb/core.log"])))

(defn fauna-test
  "Given an options map from the command line
  runner (e.g. :nodes, :ssh, :concurrency, ...), constructs a test
  map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "faunadb"
          :os    debian/os ;; NB. requires Ubuntu 14.04 LTS
          :db    (db "2.5.0-0")}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for browsing result."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn fauna-test})
                   (cli/serve-cmd))
            args))
