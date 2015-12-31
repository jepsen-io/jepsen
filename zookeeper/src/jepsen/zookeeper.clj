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
             [util :refer [timeout]]
             [nemesis :as nemesis]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [avout.core :as avout]))

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

(defn client
  [conn a]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (avout/connect (name node))
            a    (avout/zk-atom conn "/jepsen" 0)]
        (client conn a)))

    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read  (assoc op :type :ok, :value @a)
                 :write (do (avout/reset!! a (:value op))
                            (assoc op :type :ok))
                 :cas   (let [[value value'] (:value op)
                              type           (atom :fail)]
                          (avout/swap!! a (fn [current]
                                            (if (= current value)
                                              (do (reset! type :ok)
                                                  value')
                                              (do (reset! type :fail)
                                                  current))))
                          (assoc op :type @type)))))

    (teardown! [_ test]
      (.close conn))))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn simple-test
  [version]
  (assoc tests/noop-test
         :name "zookeeper"
         :os   debian/os
         :db   (db version)
         :generator (->> (gen/mix [r w cas])
                         (gen/stagger 1)
                         (gen/nemesis
                           (gen/seq (cycle [(gen/sleep 5)
                                            {:type :info, :f :start}
                                            (gen/sleep 5)
                                            {:type :info, :f :stop}])))
                         (gen/time-limit 15))
         :nemesis (nemesis/partition-random-halves)
         :client  (client nil nil)
         :model   (model/cas-register 0)
         :checker checker/linearizable))
