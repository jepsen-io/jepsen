(ns jepsen.system.datomic
  (:require [clojure.tools.logging :refer [debug info warn]]
            [clojure.java.io       :as io]
            [clojure.string        :as str]
            [jepsen.core           :as core]
            [jepsen.util           :refer [meh timeout]]
            [jepsen.codec          :as codec]
            [jepsen.core           :as core]
            [jepsen.control        :as c]
            [jepsen.control.net    :as net]
            [jepsen.control.util   :as cu]
            [jepsen.client         :as client]
            [jepsen.db             :as db]
            [jepsen.generator      :as gen]
            [knossos.core          :as knossos]
            [datomic.api           :as d])
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

    (c/exec :echo (str (slurp (io/resource "zk/zoo.cfg"))
                       "\n"
                       (zoo-cfg-servers test))
            :> "/etc/zookeeper/conf/zoo.cfg")

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
           (c/lit "/var/log/zookeeper/*")))
  (info node "ZK nuked"))

(defn riak-node-name
  [node]
  (str "riak@" (name node) ".local"))

(defn setup-riak!
  "Sets up riak"
  [test node]
  (c/su
    ; Install
    (let [file "riak_2.0.0pre20-1_amd64.deb"]
      (c/cd "/tmp"
          (when-not (cu/file? file)
            (info node "Fetching riak package")
            (c/exec :wget (str "http://s3.amazonaws.com/downloads.basho.com/riak/2.0/2.0.0pre20/debian/7/" file)))

          (try (c/exec :dpkg-query :-l :riak)
               (catch RuntimeException _
                 (info node "Installing Riak")
                 (c/exec :dpkg :-i file)))))

    ; Config
    (c/exec :echo (-> "riak/riak.conf"
                      io/resource
                      slurp
                      ; Ugh, .local is a hack cuz riak doesn't use short
                      ; node names. Gotta figure out what cuttlefish arg
                      ; to change.
                      (str/replace "%%NODE%%" (riak-node-name node)))
            :> "/etc/riak/riak.conf")

    ; Start
    (info node "starting riak")
    (c/exec :service :riak :restart)

    ; Join
    (core/synchronize test)
    (let [p (core/primary test)]
      (when-not (= node p)
        (info node "joining" p)
        (c/exec :riak-admin :cluster :join (riak-node-name p)))

      (when (= node p)
        (info "Waiting for riak convergence")
        (loop []
          (Thread/sleep 1000)
          (let [plan (c/exec :riak-admin :cluster :plan)
                valid (re-find #"\nValid:(\d+)\s" plan)]
            (when (or (nil? valid)
                      (not= (Long. (nth valid 1)) (count (:nodes test))))
              ; Still waiting for other nodes
              (recur))))

        (info node "committing")
        (info (c/exec :riak-admin :cluster :commit))

        (info node "riak ready")
        (info (c/exec :riak-admin :member-status))))))

(defn teardown-riak!
  [test node]
  (c/su
    (meh (c/exec :killall :-9 "beam.smp"))
    (meh (c/exec :killall :-9 "epmd"))
    (c/exec :rm :-rf (c/lit "/var/lib/riak/*"))
    (info node "riak nuked")))

(def riak-bucket "datomic")

(defn setup-datomic!
  [test node]
  ; Download transactor
  (let [version "0.9.4707"
        file    (str "datomic-pro-" version ".zip")
        user    "aphyr@aphyr.com"
        ; Sorry, I can't provide these, but you can ask the Datomic team.
        dkey    (str/trim (slurp (io/resource "datomic/download-key")))]
    (c/cd "/tmp"
          (when-not (cu/file? file)
            (info node "downloading datomic")
            (c/exec :wget (str "--http-user=" user)
                    (str "--http-password="   dkey)
                    (str "https://my.datomic.com/repo/com/datomic/datomic-pro/"
                         version "/" file))))

    ; Extract
    (c/cd "/opt"
          (when-not (cu/file? "datomic")
            (info node "Extracting datomic")
            (c/su
              (c/exec :unzip (str "/tmp/" file))
              (c/exec :mv (str "datomic-pro-" version) "datomic")))))

  ; Configure
  (c/su
    (c/exec :echo (slurp (io/resource "datomic/init"))
            :> "/etc/init.d/datomic")
    (c/exec :chmod "0755" "/etc/init.d/datomic")
    (c/exec :echo (-> "datomic/riak-transactor.properties"
                      io/resource
                      slurp
                      (str/replace "%%IP%%" (net/local-ip)))
            :> "/opt/datomic/config/riak-transactor.properties"))

  ; Install ZK pointer in Riak bucket
  (when (= node (core/primary test))
    (info "Writing ZK config to riak")
    (let [zk (->> test :nodes (map #(str (name %) ":2181")) (str/join ","))
          content-type "Content-Type: text/plain; charset=utf-8"
          k  "config\\zookeeper"
          opts "dw=3&pw=3"
          uri (str "http://" (name node) ":8098/riak/"
                   riak-bucket "/" k "?" opts)]

      ; Cluster probably isn't ready yet, so we spin until write success
      (loop []
        (when (try
                (c/exec :curl :-v :-f :-d zk :-H content-type uri)
                false
                (catch Exception e true))
          (Thread/sleep 1000)
          (recur)))

      (info :zk-config (c/exec :curl :-f (str "http://" (name node)
                                              ":8098/riak/" riak-bucket "/"
                                              k)))
      (info "ZK config written")))

  ; Start
  (core/synchronize test)
  (info node "starting Datomic")
  (c/su (c/exec :service :datomic :start))
  (info node "Datomic started")

  (Thread/sleep 10000000))

(defn teardown-datomic!
  [test node]
  (c/su (meh (c/exec :killall :-9 :java)))
  (info node "Datomic nuked"))

(def db
  (reify db/DB
    (setup! [_ test node]
      (setup-zk! test node)
      (setup-riak! test node)
      (setup-datomic! test node))

    (teardown! [_ test node]
      (teardown-zk! test node)
      (teardown-riak! test node)
      (teardown-datomic! test node)
      )))

(def db-name "jepsen")

(defrecord CASRegisterClient [conn]
  client/Client
  (setup! [_ test node]
    (let [uri (str "datomic:riak://" (name node) "/" riak-bucket "/" db-name)]
      (d/create-database uri)
      (CASRegisterClient (d/connect uri))))

  (teardown! [_ test]
    (d/release conn))

  (invoke! [this test op]))
