(ns jepsen.zookeeper
  (:gen-class)
  (:require [avout.core         :as avout]
            [clojure.tools.logging :refer :all]
            [clojure.java.io    :as io]
            [clojure.string     :as str]
            [jepsen [db         :as db]
                    [cli        :as cli]
                    [checker    :as checker]
                    [client     :as client]
                    [control    :as c]
                    [generator  :as gen]
                    [nemesis    :as nemesis]
                    [tests      :as tests]
                    [util       :refer [timeout]]]
            [jepsen.os.debian   :as debian]
            [knossos.model      :as model]))

(defn zk-node-ids
  "Returns a map of node names to node ids."
  [test]
  (->> test
       :nodes
       (map-indexed (fn [i node] [node i]))
       (into {})))

(defn zk-node-id
  "Given a test and a node name from that test, returns the ID for that node."
  [test node]
  ((zk-node-ids test) node))

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
        (info node "installing ZK" version)
        (debian/install {:zookeeper version
                         :zookeeper-bin version
                         :zookeeperd version})

        (c/exec :echo (zk-node-id test node) :> "/etc/zookeeper/conf/myid")

        (c/exec :echo (str (slurp (io/resource "zoo.cfg"))
                           "\n"
                           (zoo-cfg-servers test))
                :> "/etc/zookeeper/conf/zoo.cfg")

        (info node "ZK restarting")
        (c/exec :service :zookeeper :stop)
        (c/exec :service :zookeeper :start) ; for some reason, the restart often fails.
        (info node "ZK ready")))

    (teardown! [_ test node]
      (info node "tearing down ZK")
      (c/su
        (c/exec :service :zookeeper :stop) ; we must first comment this line to let the node install zk.
        (c/exec :rm :-rf
                (c/lit "/var/lib/zookeeper/version-*")
                (c/lit "/var/log/zookeeper/*"))))

    db/LogFiles
    (log-files [_ test node]
      ["/var/log/zookeeper/zookeeper.log"])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn client
  "A client for a single compare-and-set register"
  [conn a]
  (reify client/Client
    (open! [_ test node]
      (let [conn (avout/connect (name node))
            a    (avout/zk-atom conn "/jepsen" 0)]
        (client conn a)))

    (setup! [this test])

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

    (close! [_ test]
      (.close conn))

    (teardown! [_ test]
      (.close conn))))

(defn zk-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info "Creating test" opts)
  (merge tests/noop-test
         opts
         {:name    "zookeeper"
          :os      debian/os
          :db      (db "3.4.13-2")
          :client  (client nil nil)
          :nemesis (nemesis/partition-random-halves)
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis
                           (cycle [(gen/sleep 5)
                                   {:type :info, :f :start}
                                   (gen/sleep 5)
                                   {:type :info, :f :stop}]))
                          (gen/time-limit 15))
          :model   (model/cas-register 0)
          :checker (checker/compose
                    {:perf   (checker/perf)
                     :linear (checker/linearizable 
                              {:model (model/cas-register)
                               :algorithm :linear})})}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn zk-test})
                   (cli/serve-cmd))
           args))
