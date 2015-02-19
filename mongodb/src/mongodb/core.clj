(ns mongodb.core
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [knossos.core :as knossos]
            [cheshire.core :as json]
            [monger.core :as mongo]
            [monger.collection :as mc]
            [monger.result :as mr]
            [monger.command]
            [monger.conversion])
  (:import (org.bson BasicBSONObject
                     BasicBSONDecoder)
           (org.bson.types ObjectId)
           (com.mongodb DB WriteConcern)))

(defn install!
  "Installs the given version of MongoDB."
  [node version]
  (when-not (debian/installed? "mongodb-org")
    (c/su
      (try
        (info node "installing mongodb")
        (debian/install {:mongodb-org version})

        (catch RuntimeException e
          ; Add apt key
          (c/exec :apt-key     :adv
                  :--keyserver "keyserver.ubuntu.com"
                  :--recv      "7F0CEB10")

          ; Add repo
          (c/exec :echo "deb http://downloads-distro.mongodb.org/repo/debian-sysvinit dist 10gen"
                  :> "/etc/apt/sources.list.d/mongodb.list")
          (c/exec :apt-get :update)

          ; Try install again
          (debian/install {:mongodb-org version})

      ; Make sure we don't start at startup
      (c/exec :update-rc.d :mongod :remove :-f))))))

(defn configure!
  "Deploy configuration files to the node."
  [node]
  (c/exec :echo (-> "mongod.conf" io/resource slurp)
          :> "/etc/mongod.conf"))

(defn start!
  "Starts Mongod"
  [node]
  (info node "starting mongod")
  (c/su (c/exec :service :mongod :start)))

(defn stop!
  "Stops Mongod"
  [node]
  (info node "stopping mongod")
  (c/su (c/exec :service :mongod :stop)))

(defn wipe!
  "Shuts down MongoDB and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
    (c/exec :rm :-rf "/var/lib/mongodb/*")))

(defn uninstall!
  "Uninstallys the current version of mongodb"
  [node]
  (info node "uninstalling mongodb")
  (c/su
    (c/exec :apt-get :remove :--purge :-y "mongodb-org")
    (c/exec :apt-get :autoremove :-y)))

(defn mongo!
  "Run a Mongo admin command. Spits back an unparsable kinda-json string,
  because go fuck yourself, that's why."
  [cmd]
  (-> (c/exec :mongo :--quiet :--eval (str "printjson(" cmd ")"))))

(defn command!
  "Wrapper for monger's completely insane admin command API where the command
  is the first (ordered!?) key in the map with a meaningless (???!) value and
  who knows what options."
  [conn db-name & cmd]
  (-> conn
      (mongo/get-db db-name)
      (mongo/command (apply array-map cmd))
      (monger.conversion/from-db-object true)))

(defn admin-command!
  "Runs a command on the admin database."
  [conn & args]
  (apply command! conn "admin" args))

(defn replica-set-status
  "Returns the current replica set status."
  [conn]
  (admin-command! conn :replSetGetStatus 1))

(defn replica-set-initiate!
  "Initialize a replica set on a node."
  [conn]
  (admin-command! conn :replSetInitiate 1))

(defn replica-set-master?
  "What's this node's replset role?"
  [conn]
  (admin-command! conn :isMaster 1))

(defn replica-set-config
  "Returns the current replset config."
  [conn]
  (-> conn
      (mongo/get-db "local")
      (mc/find-one "system.replset" {})))

(defn replica-set-reconfigure!
  "Apply new configuration for a replica set."
  [conn conf]
  (info "Reconfiguring" conf)
  (admin-command! conn :replSetReconfig conf))

(defn optime
  "The current optime"
  [conn]
  (admin-command! conn :getoptime))

(defn node+port->node
  "Take a mongo \"n1:27107\" string and return just the node as a keyword:
  :n1."
  [s]
  (keyword ((re-find #"(\w+?):" s) 1)))

(defn primaries
  "What nodes does this conn think are primaries?"
  [conn]
  (->> (replica-set-status conn)
       :members
       (filter #(= "PRIMARY" (:stateStr %)))
       (map :name)
       (map node+port->node)))

(defn primary
  "Which single node does this conn think the primary is? Throws for multiple
  primaries, cuz that sounds like a fun and interesting bug, haha."
  [conn]
  (let [ps (primaries conn)]
    (when (< 1 (count ps))
      (throw (IllegalStateException.
               (str "Multiple primaries known to "
                    conn
                    ": "
                    ps))))

    (first ps)))

(defn await-primary
  "Block until a primary is known to the current node."
  [conn]
  (while (not (primary conn))
    (Thread/sleep 1000)))

(defn join!
  "Join nodes into a replica set. Blocks until a primary is visible to all
  nodes."
  [test node]
  (with-open [conn (mongo/connect {:host (name node)})]
    ; Initiate RS
    (replica-set-initiate! conn)
    (jepsen/synchronize test)

    (when (= node (jepsen/primary test))
      (info node "Config is" (replica-set-config conn))

      ; Reconfigure primary
      (replica-set-reconfigure!
        conn
        (-> (replica-set-config conn)
            (update-in [:version] inc)
            (assoc :members
                   (->> test
                        :nodes
                        (map-indexed (fn [i node]
                                       {:_id  i
                                        :host (str (name node) ":27017")})))))))

    ; Wait for everyone to catch up
    (info node "waiting for primary")
    (await-primary conn)
    (info node "primary is" (primary conn))
    (jepsen/synchronize test)))

(defn db [version]
  "MongoDB for a particular version."
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)
        (configure!)
        (start!))
      (join! test node))

    (teardown! [_ test node]
;      (wipe! node))))
      )))

;; Document CAS

(defrecord DocumentCASClient [db-name coll id write-concern conn db]
  client/Client
  (setup! [this test node]
    ; Connect to entire replica set
    (let [conn (mongo/connect (->> test
                                   :nodes
                                   (map name)
                                   (mapv mongo/server-address)))
          db   (mongo/get-db conn db-name)]

      (Thread/sleep 5000)

      ; Create document
      (mc/upsert db coll
                 {:_id id}
                 {:_id id, :value nil}
                 {:write-concern write-concern})

      (assoc this :conn conn, :db db)))

  (invoke! [this test op]
    ; Reads are idempotent; we can treat their failure as an info.
    (let [fail (if (= :read (:f op))
                 :fail
                 :info)]
      (case (:f op)
        :read  (:value (mc/find-map-by-id db coll id))
        :write (mc/update-by-id db coll id
                                {:_id id, :value (:value op)}
                                {:write-concern write-concern})
        :cas   (let [[value value'] (:value op)]
                 (mc/update db coll id
                            {:_id id, :value value}
                            {:_id id, :value value'}
                            {:write-concern write-concern})))))

  (teardown! [_ test]
    (mongo/disconnect conn)))

(defn document-cas-client
  "A client which implements a register on top of an entire document."
  []
  (DocumentCASClient. "jepsen"
                      "jepsen"
                      (ObjectId.)
                      WriteConcern/JOURNAL_SAFE
                      nil
                      nil))

(defn document-cas-test
  "Document-level compare and set."
  []
  (assoc tests/noop-test
         :name      "mongodb document cas"
         :os        debian/os
         :db        (db "2.6.7")
         :client    (document-cas-client)
         :model     (model/cas-register)
         :checker   checker/linearizable
         :nemesis   (nemesis/partition-random-halves)
         :generator (gen/phases
                      (->> gen/cas
                           (gen/delay 1)
                           (gen/nemesis
                             (gen/seq (cycle [(gen/sleep 30)
                                              {:type :info :f :start}
                                              (gen/sleep 30)
                                              {:type :info :f :stop}])))
                           (gen/time-limit 20))
                      (gen/nemesis
                        (gen/once {:type :info :f :stop}))
                      (gen/clients
                        (gen/once {:type :invoke :f :read})))))
