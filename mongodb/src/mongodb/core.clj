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
            [monger.conversion :refer [from-db-object]])
  (:import (clojure.lang ExceptionInfo)
           (org.bson BasicBSONObject
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
  (c/su
    (c/exec :service :mongod :stop))
    (meh (c/exec :killall :-9 :mongod)))

(defn wipe!
  "Shuts down MongoDB and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
    (c/exec :rm :-rf (c/lit "/var/lib/mongodb/*"))))

(defn uninstall!
  "Uninstallys the current version of mongodb"
  [node]
  (info node "uninstalling mongodb")
  (c/su
    (c/exec :apt-get :remove :--purge :-y "mongodb-org")
    (c/exec :apt-get :autoremove :-y)))

(defn mongo!
  "Run a Mongo shell command. Spits back an unparsable kinda-json string,
  because go fuck yourself, that's why."
  [cmd]
  (-> (c/exec :mongo :--quiet :--eval (str "printjson(" cmd ")"))))

(defn parse-result
  "Takes a Mongo result and returns it as a Clojure data structure (using
  mongo.result/from-db-object), unless it contains an error, in which case an
  ex-info exception of :type :mongo is raised."
  [res]
  (if (mr/ok? res)
    (from-db-object res true)
    (throw (ex-info (str "Mongo error: "
                         (get res "errmsg")
                         " - "
                         (get res "info"))
                    {:type   :mongo
                     :result res}))))

(defn command!
  "Wrapper for monger's admin command API where the command
  is the first key in the (ordered!?) map."
  [conn db-name & cmd]
;  (info "command" (seq (.getServerAddressList conn)) db-name cmd)
  (timeout 10000 (throw (ex-info "timeout" {:db-name db-name :cmd cmd}))
           (-> conn
               (mongo/get-db db-name)
               (mongo/command (apply array-map cmd))
               parse-result)))

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
  [conn config]
  (try
    (admin-command! conn :replSetInitiate config)
    (catch ExceptionInfo e
      ; Some of the time (but not all the time; why?) Mongo returns this error
      ; from replsetinitiate, which is, as far as I can tell, not actually an
      ; error (?)
      (when-not (= "Received replSetInitiate - should come online shortly."
                   (get-in (ex-data e) [:result "errmsg"]))
        (throw e)))))

(defn replica-set-master?
  "What's this node's replset role?"
  [conn]
  (admin-command! conn :isMaster 1))

(defn replica-set-config
  "Returns the current replset config."
  [conn]
  (-> conn
      (mongo/get-db "local")
      (mc/find-one "system.replset" {})
      (from-db-object true)))

(defn replica-set-reconfigure!
  "Apply new configuration for a replica set."
  [conn conf]
  (admin-command! conn :replSetReconfig conf))

(defn optime
  "The current optime"
  [conn]
  (admin-command! conn :getoptime 1))

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

(def mongo-conn-opts
  "Connection options for Mongo."
  (mongo/mongo-options
    {:max-wait-time   10000
     :connect-timeout 5000
     :socket-timeout  5000}))  ; a buncha simple ops in mongo take 1000+ ms (!?)

(defn await-conn
  "Block until we can connect to the given node. Returns a connection to the
  node."
  [node]
  (or (try
        (let [conn (mongo/connect (mongo/server-address (name node))
                                  mongo-conn-opts)]
          (try
            (optime conn)
            conn
            (catch Throwable t
              (mongo/disconnect conn)
              (throw t))))
        (catch com.mongodb.MongoServerSelectionException e
          nil))
      (do
        (Thread/sleep 1000)
        (recur node))))

(defn await-primary
  "Block until a primary is known to the current node."
  [conn]
  (while (not (primary conn))
    (Thread/sleep 1000)))

(defn await-join
  "Block until all nodes in the test are known to this connection's replset
  status"
  [test conn]
  (while (not= (set (:nodes test))
               (->> (replica-set-status conn)
                    :members
                    (map :name)
                    (map node+port->node)
                    set))
    (Thread/sleep 1000)))

(defn target-replica-set-config
  "Generates the config for a replset in a given test."
  [test]
  {:_id "jepsen"
   :members (->> test
                 :nodes
                 (map-indexed (fn [i node]
                                {:_id  i
                                 :host (str (name node) ":27017")})))})

(defn join!
  "Join nodes into a replica set. Blocks until any primary is visible to all
  nodes which isn't really what we want but oh well."
  [test node]
  ; Gotta have all nodes online for this
  (jepsen/synchronize test)

  ; Initiate RS
  (when (= node (jepsen/primary test))
    (with-open [conn (await-conn node)]
      (info node "Initiating replica set")
      (replica-set-initiate! conn (target-replica-set-config test))

      (info node "Jepsen primary waiting for cluster join")
      (await-join test conn)
      (info node "Jepsen primary waiting for mongo election")
      (await-primary conn)
      (info node "Primary ready.")))

  ; For reasons I really don't understand, you have to prevent other nodes
  ; from checking the replset status until *after* we initiate the replset on
  ; the primary--so we insert a barrier here to make sure other nodes don't
  ; wait until primary initiation is complete.
  (jepsen/synchronize test)

  ; For other reasons I don't understand, you *have* to open a new set of
  ; connections after replset initation. I have a hunch that this happens
  ; because of a deadlock or something in mongodb itself, but it could also
  ; be a client connection-closing-detection bug.

  ; Amusingly, we can't just time out these operations; the client appears to
  ; swallow thread interrupts and keep on doing, well, something. FML.
  (with-open [conn (await-conn node)]
    (info node "waiting for cluster join")
    (await-join test conn)

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
      (wipe! node))))
;      )))

;; Document CAS

(defrecord DocumentCASClient [db-name coll id write-concern conn db]
  client/Client
  (setup! [this test node]
    ; Connect to entire replica set
    (let [conn (mongo/connect (->> test
                                   :nodes
                                   (map name)
                                   (mapv mongo/server-address))
                              mongo-conn-opts)
          db   (mongo/get-db conn db-name)]

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
      (try
        (case (:f op)
          ; Note that find-by-id and find-map-by-id do NOT return wrapper
          ; objects with :ok/:err fields, so we can't check if they're OK or
          ; not, AUGH
          ;
          ; What do they return in an error? Do they throw?
          :read (let [res (mc/find-map-by-id db coll id)]
                  (assoc op :type :ok, :value (:value res)))

          :write (let [res (parse-result
                             (mc/update-by-id db coll id
                                              {:_id id, :value (:value op)}
                                              {:write-concern write-concern}))]
                   (assert (= 1 (.getN res)))
                   (assoc op :type :ok))

          :cas   (let [[value value'] (:value op)
                       res (parse-result
                             (mc/update db coll
                                        {:_id id, :value value}
                                        {:_id id, :value value'}
                                        {:write-concern write-concern}))]
                   ; Check how many documents we actually modified...
                   (condp = (.getN res)
                     0 (assoc op :type :fail)
                     1 (assoc op :type :ok)
                     2 (throw (ex-info "CAS unexpected number of modified docs"
                                       {:n (.getN res)
                                        :res res})))))

        ; A server selection error means we never attempted the operation in
        ; the first place, so we know it didn't take place.
        (catch com.mongodb.MongoServerSelectionException e
          (assoc op :type :fail :value :no-server))

        ; Could be incomplete, but we always treat reads as failures
        (catch com.mongodb.MongoException$Network e
          (assoc op :type fail :value :network-error)))))

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
         :checker   (checker/compose {:linear checker/linearizable})
         :nemesis   (nemesis/partition-random-halves)
         :generator (gen/phases
                      (->> gen/cas
                           (gen/delay 1)
                           (gen/nemesis
                             (gen/seq (cycle [(gen/sleep 30)
                                              {:type :info :f :start}
                                              (gen/sleep 30)
                                              {:type :info :f :stop}])))
                           (gen/time-limit 30))
                      (gen/nemesis
                        (gen/once {:type :info :f :stop}))
                      (gen/clients
                        (gen/once {:type :invoke :f :read})))))
