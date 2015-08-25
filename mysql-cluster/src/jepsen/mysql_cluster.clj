(ns jepsen.mysql-cluster
  "Tests for MySQL Cluster!"
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [jepsen [client :as client]
             [core :as jepsen]
             [db :as db]
             [tests :as tests]
             [control :as c :refer [|]]
             [checker :as checker]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :refer [timeout meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]))

(defn deb-version
  "Version of package in a deb file."
  [deb-file]
  (c/exec :dpkg-deb :-f deb-file :Version))

(defn deb-package
  "Package name in a deb file."
  [deb-file]
  (c/exec :dpkg-deb :-f deb-file :Package))

(defn deb-install!
  "Install a deb file if it's not already installed."
  [deb-file]
  (let [package (deb-package deb-file)
        version (deb-version deb-file)]
    (when (not= version (debian/installed-version package))
      (info "Installing" deb-file)
      (c/su (c/exec :dpkg :-i :--force-confask :--force-confnew deb-file)))))

(defn install!
  "Downloads and installs the mysql cluster packages."
  [node version]
  (c/su
    (c/cd "/tmp"
          (deb-install! (cu/wget! (str "https://dev.mysql.com/get/Downloads/"
                                       "MySQL-Cluster-7.4/mysql-cluster-gpl-"
                                       version
                                       "-debian7-x86_64.deb"))))))

(def mgmd-dir             "/var/lib/mysql/cluster")
(def ndbd-dir             "/var/lib/mysql/data")

(defn nbd-mgmd-conf
  "Config snippet for a management node"
  [node-id node]
  (str "[ndb_mgmd]\n"
       "id=" (swap! node-id inc) "\n"
       "hostname=" (name node) "\n"
       "datadir=" mgmd-dir "\n"))

(defn ndbd-conf
  "Config snippet for a storage node"
  [node-id node]
  (str "[ndbd]\n"
       "id=" (swap! node-id inc) "\n"
       "hostname=" (name node) "\n"
       "datadir=" ndbd-dir "\n"))

(defn mysqld-conf
  "Config snippet for a mysql node"
  [node-id node]
  (str "[mysqld]\n"
       "id=" (swap! node-id inc) "\n"
       "hostname=" (name node) "\n"))

(defn ndbd-nodes
  "Given a test, returns a sorted set of ndbd nodes"
  [test]
  (into (sorted-set) (take 4 (:nodes test))))

(defn nodes-conf
  "Config snippet for all roles on all nodes."
  [test]
  (let [nodes (:nodes test)
        nbds  (ndbd-nodes test)
        node-id (atom -1)]
    (str/join "\n"
              (mapcat (fn [gen nodes]
                        (map (partial gen node-id) nodes))
                      [nbd-mgmd-conf ndbd-conf mysqld-conf]
                      [nodes         nbds      nodes]))))

(defn ndb-connect-string
  "Constructs an ndb connection string for a test."
  [test]
  (str/join "," (map name (:nodes test))))

(defn configure!
  "Sets up config files"
  [test node]
  (c/su
    ; my.cnf
    (c/exec :echo (-> (io/resource "my.cnf")
                      slurp
                      (str/replace #"%NDB_CONNECT_STRING%"
                                   (ndb-connect-string test)))
            :> "/etc/my.cnf")

    ; config.ini
    (c/exec :mkdir :-p mgmd-dir)
    (c/exec :echo (-> (io/resource "config.ini")
                      slurp
                      (str (nodes-conf test)))
            :> (str mgmd-dir "/config.ini"))))

(defn start-mgmd!
  "Starts management daemon."
  [node]
  (info node "starting mgmd")
  (c/su (c/exec "/opt/mysql/server-5.6/bin/ndb_mgmd"
                :-f (str mgmd-dir "/config.ini"))))

(defn start-ndbd!
  "Starts storage daemon."
  [test node]
  (when (contains? (ndbd-nodes test) node)
    (info node "starting ndbd")
    (c/su
      (c/exec :mkdir :-p ndbd-dir)
      (c/exec "/opt/mysql/server-5.6/bin/ndbd"))))

(defn start!
  "Starts all services on a node."
  [test node]
  (start-mgmd! node)
  (jepsen/synchronize test)
  (start-ndbd! test node))

(defn stop-mgmd!
  "Stops management daemon."
  [node]
  (info node "stopping mgmd")
  (meh (cu/grepkill "ndb_mgmd")))

(defn stop-ndbd!
  "Stops storage daemon."
  [node]
  (info node "stopping ndbd")
  (meh (cu/grepkill "ndbd")))

(defn wipe!
  "Nukes mysql cluster on this node."
  [node]
  (stop-ndbd! node)
  (stop-mgmd! node)
  (c/su (c/exec :rm :-rf
                (c/lit (str mgmd-dir "/*"))
                (c/lit (str ndbd-dir "/*")))))

(defn db
  "Sets up and tears down MySQL Cluster."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (install! node version)
      (configure! test node)
      (start! test node)
      (Thread/sleep 20000))

    (teardown! [_ test node]
      (wipe! node))

    db/LogFiles
    (log-files [_ test node]
      (filter (partial re-find #"\.log$")
              (cu/ls-full mgmd-dir)))))

(defn simple-test
  [version]
  (assoc tests/noop-test
         :name "mysql"
         :os   debian/os
         :db   (db version)))
