(ns jepsen.mongodb-smartos.core
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
                    [net]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.smartos :as smartos]
            [jepsen.checker.timeline :as timeline]
            [knossos.core :as knossos]
            [cheshire.core :as json]
            [monger.core :as mongo]
            [monger.collection :as mc]
            [monger.result :as mr]
            [monger.query :as mq]
            [monger.command :as command]
            [monger.operators :refer :all]
            [monger.conversion :refer [from-db-object]])
  (:import (clojure.lang ExceptionInfo)
           (org.bson BasicBSONObject
                     BasicBSONDecoder)
           (org.bson.types ObjectId)
           (com.mongodb DB
                        WriteConcern
                        ReadPreference)))

(defn install!
  "Installs the given version of MongoDB."
  [node db-version tools-version]
  (c/su
   (smartos/install {:mongodb db-version
                     :mongo-tools tools-version})
   (c/exec :mkdir :-p "/var/lib/mongodb")
   (c/exec :chown :-R "mongodb:mongodb" "/var/lib/mongodb")))

(defn configure!
  "Deploy configuration files to the node."
  [node]
  (c/exec :echo (-> "mongod.conf" io/resource slurp)
          :> "/opt/local/etc/mongod.conf"))

(defn start!
  "Starts Mongod"
  [node]
  (info node "starting mongod")
  (c/su
   (meh (c/exec :svcadm :clear :mongodb))
   (c/exec :svcadm :enable :-r :mongodb)))

(defn stop!
  "Stops Mongod"
  [node]
  (info node "stopping mongod")
  (c/su
    (timeout 5000 nil
             (meh (c/exec :svcadm :disable :mongodb)))
    (meh (c/exec :pkill :-9 :mongod))))

(defn wipe!
  "Shuts down MongoDB and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files")
  (c/su
    (c/exec :rm :-rf (c/lit "/var/log/mongodb/*"))
    (c/exec :rm :-rf (c/lit "/var/lib/mongodb/*"))))

(defn uninstall!
  "Uninstalls the current version of mongodb"
  [node]
  (info node "uninstalling mongodb")
  (smartos/uninstall! ["mongo" "mongo-tools"]))

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
      (condp re-find (get-in (ex-data e) [:result "errmsg"])
        ; Some of the time (but not all the time; why?) Mongo returns this error
        ; from replsetinitiate, which is, as far as I can tell, not actually an
        ; error (?)
        #"Received replSetInitiate - should come online shortly"
        nil

        ; This is a hint we should back off and retry; one of the nodes probably
        ; isn't fully alive yet.
        #"need all members up to initiate, not ok"
        (do (info "not all members alive yet; retrying replica set initiate"
            (Thread/sleep 1000)
            (replica-set-initiate! conn config)))

        ; Or by default re-throw
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
    {:max-wait-time   20000
     :connect-timeout 5000
     :socket-timeout  10000})) ; a buncha simple ops in mongo take 1000+ ms (!?)

(defn await-conn
  "Block until we can connect to the given node. Returns a connection to the
  node."
  [node]
  (timeout (* 100 1000)
           (throw (ex-info "Timed out trying to connect to MongoDB"
                           {:node node}))
           (loop []
             (or (try
                   (let [conn (mongo/connect (mongo/server-address (name node))
                                             mongo-conn-opts)]
                     (try
                       (command/top conn)
                       conn
                       (catch Throwable t
                         (mongo/disconnect conn)
                         (throw t))))
                   (catch com.mongodb.MongoServerSelectionException e
                     nil))
                 (do
                   (Thread/sleep 1000)
                   (recur))))))

(defn await-primary
  "Block until a primary is known to the current node."
  [conn]
  (while (not (primary conn))
    (Thread/sleep 1000)))

(defn await-join
  "Block until all nodes in the test are known to this connection's replset
  status"
  [test conn]
  (while (try (not= (set (:nodes test))
                    (->> (replica-set-status conn)
                         :members
                         (map :name)
                         (map node+port->node)
                         set))
              (catch ExceptionInfo e
                (if (re-find #"should come online shortly"
                             (get-in (ex-data e) [:result "errmsg"]))
                  true
                  (throw e))))
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
  [node test]
  ; Gotta have all nodes online for this
  (jepsen/synchronize test)
  (Thread/sleep 10000)

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

(defn db
  "MongoDB for a particular version."
  [db-version tools-version]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! db-version tools-version)
        (configure!)
        (start!)
        (join! test)))

    (teardown! [_ test node]
      (wipe! node))))

(defn cluster-client
  "Returns a mongoDB connection for all nodes in a test."
  [test]
  (mongo/connect (->> test :nodes (map name) (mapv mongo/server-address))
                 mongo-conn-opts))

(defn upsert
  "Ensures the existence of the given document."
  [db coll doc write-concern]
  (assert (:_id doc))
  (mc/upsert db coll
             {:_id (:_id doc)}
             doc
             {:write-concern write-concern}))

(defmacro with-errors
  "Takes an invocation operation, a set of idempotent operation functions which
  can be safely assumed to fail without altering the model state, and a body to
  evaluate. Catches MongoDB errors and maps them to failure ops matching the
  invocation."
  [op idempotent-ops & body]
  `(let [error-type# (if (~idempotent-ops (:f ~op))
                       :fail
                       :info)]
     (try
       ~@body
       ; A server selection error means we never attempted the operation in
       ; the first place, so we know it didn't take place.
       (catch com.mongodb.MongoServerSelectionException e#
         (assoc ~op :type :fail :error :no-server))

       ; Command failures also did not take place.
       (catch com.mongodb.CommandFailureException e#
         (if (= "not master" (.. e# getCommandResult getErrorMessage))
           (assoc ~op :type :fail :error :not-master)
           (throw e#)))

       ; A network error is indeterminate
       (catch com.mongodb.MongoException$Network e#
         (assoc ~op :type error-type# :error :network-error)))))

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  [gen]
  (gen/phases
    (->> gen
         (gen/delay 1)
         (gen/nemesis
           (gen/seq (cycle [(gen/sleep 60)
                            {:type :info :f :stop}
                            {:type :info :f :start}])))
         (gen/time-limit 600))
    ; Recover
    (gen/nemesis
      (gen/once {:type :info :f :stop}))
    ; Wait for resumption of normal ops
    (gen/clients
      (->> gen
           (gen/delay 1)
           (gen/time-limit 30)))))

(defn test-
  "Constructs a test with the given name prefixed by 'mongodb ', merging any
  given options."
  [name opts]
  (merge
    (assoc tests/noop-test
           :name      (str "mongodb " name)
           :os        smartos/os
           :net       jepsen.net/ipfilter
           :db        (db "3.0.4" "3.0.6")
           :model     (model/cas-register)
           :checker   (checker/compose {:linear checker/linearizable})
           :nemesis   (nemesis/partition-random-halves))
    opts))
