(ns jepsen.mongodb.core
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [debug info warn]]
            [clojure.walk :as walk]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb.mongo :as m]
            [knossos [core :as knossos]
                     [model :as model]])
  (:import (clojure.lang ExceptionInfo)))

(def username "mongodb")

(defn install!
  "Installs a tarball from an HTTP URL"
  [node url]
  ; Add user
  (cu/ensure-user! username)

  ; Download tarball
  (let [local-file (nth (re-find #"file://(.+)" url) 1)
        file       (or local-file (c/cd "/tmp" (str "/tmp/" (cu/wget! url))))]
    (try
      (c/cd "/opt"
            ; Clean up old dir
            (c/exec :rm :-rf "mongodb")
            ; Create mongodb & data dir
            (c/exec :mkdir :-p "mongodb/data")
            ; Extract to mongodb
            (c/exec :tar :xvf file :-C "mongodb" :--strip-components=1)
            ; Permissions
            (c/exec :chown :-R (str username ":" username) "mongodb"))
    (catch RuntimeException e
      (condp re-find (.getMessage e)
        #"tar: Unexpected EOF"
        (if local-file
          ; Nothing we can do to recover here
          (throw (RuntimeException.
                   (str "Local tarball " local-file " on node " (name node)
                        " is corrupt: unexpected EOF.")))
          (do (info "Retrying corrupt tarball download")
              (c/exec :rm :-rf file)
              (install! node url)))

        ; Throw by default
        (throw e))))))

(defn configure!
  "Deploy configuration files to the node."
  [test node]
  (c/exec :echo (-> "mongod.conf" io/resource slurp
                    (str/replace #"%STORAGE_ENGINE%" (:storage-engine test))
                    (str/replace #"%PROTOCOL_VERSION%" (:protocol-version test)))
          :> "/opt/mongodb/mongod.conf"))

(defn start!
  "Starts Mongod"
  [test node]
  (c/sudo username
          (cu/start-daemon! {:logfile "/opt/mongodb/stdout.log"
                             :pidfile "/opt/mongodb/pidfile"
                             :remove-pidfile? false
                             :chdir   "/opt/mongodb"}
                            "/opt/mongodb/bin/mongod"
                            :--config "/opt/mongodb/mongod.conf"))
  :started)

(defn stop!
  "Stops Mongod"
  [test node]
  (cu/stop-daemon! "mongod" "/opt/mongodb/pidfile")
  :stopped)

(defn savelog!
  "Saves Mongod log"
  [node]
  (info node "copying mongod.log & stdout.log file to /root/")
  (c/su
    (c/exec :touch "/opt/mongodb/mongod.log" "/opt/mongodb/stdout.log")
    (c/exec :cp :-f "/opt/mongodb/mongod.log" "/opt/mongodb/stdout.log" "/root/")))

(defn wipe!
  "Shuts down MongoDB and wipes data."
  [test node]
  (stop! test node)
  (savelog! node)
  (info node "deleting data files")
  (c/su
    (c/exec :rm :-rf (c/lit "/opt/mongodb/*.log"))))

(defn mongo!
  "Run a Mongo shell command. Spits back an unparsable kinda-json string,
  because what else would 'printjson' do?"
  [cmd]
  (-> (c/exec :mongo :--quiet :--eval (str "printjson(" cmd ")"))))

;; Cluster setup

(defn replica-set-status
  "Returns the current replica set status."
  [conn]
  (m/admin-command! conn :replSetGetStatus 1))

(defn replica-set-initiate!
  "Initialize a replica set on a node."
  [conn config]
  (try
    (m/admin-command! conn :replSetInitiate config)
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
  (m/admin-command! conn :isMaster 1))

(defn replica-set-config
  "Returns the current replset config."
  [conn]
  (m/admin-command! conn :replSetGetConfig 1))

(defn replica-set-reconfigure!
  "Apply new configuration for a replica set."
  [conn conf]
  (m/admin-command! conn :replSetReconfig conf))

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

(defn await-conn
  "Block until we can connect to the given node. Returns a connection to the
  node."
  [node]
  (timeout (* 300 1000)
           (throw (ex-info "Timed out trying to connect to MongoDB"
                           {:node node}))
           (loop []
             (or (try
                   (let [conn (m/client node)]
                     (try
                       (.first (.listDatabaseNames conn))
                       conn
                       ; Don't leak clients when they fail
                       (catch Throwable t
                         (.close conn)
                         (throw t))))
                   (catch com.mongodb.MongoSocketReadTimeoutException e
                     (info "Mongo socket read timeout waiting for conn; retrying")
                     nil)
;                   (catch com.mongodb.MongoServerSelectionException e
;                     nil))
                    ; Todo: figure out what Mongo 3.x throws when servers
                    ; aren't ready yet
                    )
                 ; If we aren't ready, sleep and retry
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
  (assert (integer? (:protocol-version test)))
  {:_id "jepsen"
   :protocolVersion (:protocol-version test)
   :members (->> test
                 :nodes
                 (map-indexed (fn [i node]
                                {:_id  i
                                 :host (str (name node) ":27017")})))})

(defn join!
  "Join nodes into a replica set. Blocks until any primary is visible to all
  nodes which isn't really what we want but oh well."
  [test node]
  ; Gotta have all nodes online for this. Delightfully, Mongo won't actually
  ; bind to the port until well *after* the init script startup process
  ; returns. This would be fine, except that  if a node isn't ready to join,
  ; the initiating node will just hang indefinitely, instead of figuring out
  ; that the node came online a few seconds later.
  (.close (await-conn node))
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

(defn db
  "MongoDB for a particular HTTP URL"
  [url]
  (reify db/DB
    (setup! [_ test node]
      (util/timeout 300000
                    (throw (RuntimeException.
                             (str "Mongo setup on " node "timed out!")))
                    (debian/install [:libc++1 :libsnmp30])
                    (install! node url)
                    (configure! test node)
                    (start! test node)
                    (join! test node)))

    (teardown! [_ test node]
      (wipe! test node))

    db/LogFiles
    (log-files [_ test node]
      ["/opt/mongodb/stdout.log"
       "/opt/mongodb/mongod.log"])))

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
       (catch com.mongodb.MongoNotPrimaryException e#
         (assoc ~op :type :fail, :error :not-primary))

       ; A network error is indeterminate
       (catch com.mongodb.MongoSocketReadException e#
         (assoc ~op :type error-type# :error :socket-read))

       (catch com.mongodb.MongoSocketReadTimeoutException e#
         (assoc ~op :type error-type# :error :socket-read)))))

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  [gen]
  (->> gen
       (gen/stagger 1)
       (gen/nemesis
         (gen/seq (cycle [(gen/sleep 20)
                          {:type :info :f :stop}
                          {:type :info :f :start}])))))

(defn random-nonempty-subset
  [nodes]
  (take (inc (rand-int (count nodes))) (shuffle nodes)))

(defn kill-nem
  "A nemesis that kills/restarts Mongo on randomly selected nodes."
  []
  (nemesis/node-start-stopper random-nonempty-subset start!  stop!))

(defn pause-nem
  "A nemesis that pauses Mongo on randomly selected nodes."
  []
  (nemesis/hammer-time random-nonempty-subset "mongod"))

(defn test-
  "Constructs a test with the given name prefixed by 'mongodb ', merging any
  given options. Special options for Mongo:

  :tarball            HTTP URL of a tarball to install
  :time-limit         How long do we run the test for?
  :storage-engine     Storage engine to use
  :protocol-version   Replication protocol version"
  [name opts]
  (merge
    (assoc tests/noop-test
           :name            (str "mongodb " name " s:" (:storage-engine opts)
                                 " p:" (:protocol-version opts))
           :os              debian/os
           :db              (db (:tarball opts))
           :checker         (checker/perf)
           :nemesis         (pause-nem))
    opts))
