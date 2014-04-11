(ns jepsen.system.datomic
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.java.io       :as io]
            [clojure.string        :as str]
            [jepsen.core           :as core]
            [jepsen.util           :refer [meh timeout]]
            [jepsen.codec          :as codec]
            [jepsen.core           :as core]
            [jepsen.control        :as c]
            [jepsen.control.util   :as cu]
            [jepsen.client         :as client]
            [jepsen.db             :as db]
            [jepsen.generator      :as gen]
            [knossos.core          :as knossos])
  (:import (com.rabbitmq.client AlreadyClosedException
                                ShutdownSignalException)))

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

(defn setup-zk!
  "Sets up zookeeper"
  [test node]
  (c/su
    (info node "Setting up ZK")
    ; Install zookeeper
    (c/exec :apt-get :install :-y :zookeeper :zookeeper-bin :zookeeperd)

    ; Set up zookeeper
    (c/exec :echo (zk-node-id test node) :> "/etc/zookeeper/conf/myid")

    (->> (c/exec :echo (str (slurp (io/resource "zk/zoo.cfg"))
                            "\n"
                            (zoo-cfg-servers test))
                 :> "/etc/zookeeper/conf/zoo.cfg"))

    ; Restart
    (info node "ZK restarting")
    (c/exec :service :zookeeper :restart)

    (info node "ZK ready")))

(defn teardown-zk!
  "Tears down zookeeper"
  [test node]
  (c/su
    (c/exec :service :zookeeper :stop)
    (c/exec :rm :-rf
           (c/lit "/var/lib/zookeeper/version-*")
           (c/lit "/var/log/zookeeper/*"))))

(def db
  (reify db/DB
    (setup! [_ test node]
      (setup-zk! test node))

    (teardown! [_ test node]
;      (teardown-zk! test node))
      )))

