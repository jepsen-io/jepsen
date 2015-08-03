(ns jepsen.zookeeper
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen [client :as client]
             [db :as db]
             [tests :as tests]
             [control :as c :refer [|]]
             [checker :as checker]
             [generator :as gen]
             [util :refer [timeout]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn zk-node-ids
  "We number nodes in reverse order so the leader is the first node. Returns a
  map of node names to node ids."
  [test]
  (->> test
       :nodes
       (map-indexed (fn [i node] [node (- (count (:nodes test)) i)]))
       (into {})))

(defn zk-node-id
  [test node]
  (get (zk-node-ids test) node))

(defn zoo-cfg-servers
  "Constructs a zoo.cfg fragment for servers."
  [test]
  (->> (zk-node-ids test)
       (map (fn [[node id]]
              (str "server." id "=" (name node) ":2888:3888")))
       (str/join "\n")))

(defn db
  "Zookeeper DB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "Setting up ZK")
        ; Install zookeeper
        (debian/install {:zookeeper version
                         :zookeeper-bin version
                         :zookeeperd version})

        ; Set up zookeeper
        (c/exec :echo (zk-node-id test node) :> "/etc/zookeeper/conf/myid")

        (c/exec :echo (str (slurp (io/resource "zoo.cfg"))
                           "\n"
                           (zoo-cfg-servers test))
                :> "/etc/zookeeper/conf/zoo.cfg")

        ; Restart
        (info node "ZK restarting")
        (c/exec :service :zookeeper :restart)
        (info node "ZK ready")))

    (teardown! [_ test node]
      (c/su
        (c/exec :service :zookeeper :stop)
        (c/exec :rm :-rf
                (c/lit "/var/lib/zookeeper/version-*")
                (c/lit "/var/log/zookeeper/*"))))

    db/LogFiles
    (log-files [_ _ _]
      ["/var/log/zookeeper/zookeeper.log"])))

(defn simple-test
  [version]
  (assoc tests/noop-test
         :name "zookeeper"
         :os   debian/os
         :db   (db version)
         :generator (gen/sleep 30)))
