(ns tidb.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
              [core :as jepsen]
              [control :as c]
              [db :as db]]
            [jepsen.control.util :as cu]
            [tidb.sql :as sql]))

(def tidb-dir       "/opt/tidb")
(def pd-bin         "pd-server")
(def kv-bin         "tikv-server")
(def db-bin         "tidb-server")
(def pd-logfile     (str tidb-dir "/pd.log"))
(def pd-stdout      (str tidb-dir "/pd.stdout"))
(def pd-pidfile     (str tidb-dir "/pd.pid"))
(def kv-logfile     (str tidb-dir "/kv.log"))
(def kv-stdout      (str tidb-dir "/kv.stdout"))
(def kv-pidfile     (str tidb-dir "/kv.pid"))
(def db-logfile     (str tidb-dir "/db.log"))
(def db-stdout      (str tidb-dir "/db.stdout"))
(def db-pidfile     (str tidb-dir "/db.pid"))
(def pd-configfile  (str tidb-dir "/pd.conf"))
(def kv-configfile  (str tidb-dir "/tikv.conf"))

(def client-port 2379)
(def peer-port   2380)

(def tidb-map
  {"n1" {:pd "pd1" :kv "tikv1"}
   "n2" {:pd "pd2" :kv "tikv2"}
   "n3" {:pd "pd3" :kv "tikv3"}
   "n4" {:pd "pd4" :kv "tikv4"}
   "n5" {:pd "pd5" :kv "tikv5"} })

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node client-port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node peer-port))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (get-in tidb-map [node :pd]) "=" (peer-url node))))
       (str/join ",")))

(defn pd-endpoints
  "Constructs an initial pd cluster string for a test, like
  \"foo:2379,bar:2379,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (name node) ":" client-port)))
       (str/join ",")))

(defn configure-pd!
  "Writes configuration file for placement driver"
  []
  (c/su
    (c/exec :echo "[replication]\nmax-replicas=5" :> pd-configfile)))

(defn configure-kv!
  "Writes configuration file for tikv"
  []
  (c/su
    (c/exec :echo "[raftstore]\npd-heartbeat-tick-interval=\"5s\"\nraft_store_max_leader_lease=\"900ms\"\nraft_base_tick_interval=\"100ms\"\nraft_heartbeat_ticks=3\nraft_election_timeout_ticks=10" :> kv-configfile)))

(defn configure!
  "Write all config files."
  []
  (configure-pd!)
  (configure-kv!))

(defn start-pd!
  "Starts the placement driver daemon"
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile pd-stdout
       :pidfile pd-pidfile
       :chdir   tidb-dir
       }
      (str "./bin/" pd-bin)
      :--name                  (get-in tidb-map [node :pd])
      :--data-dir              (get-in tidb-map [node :pd])
      :--client-urls           (str "http://0.0.0.0:" client-port)
      :--peer-urls             (str "http://0.0.0.0:" peer-port)
      :--advertise-client-urls (client-url node)
      :--advertise-peer-urls   (peer-url node)
      :--initial-cluster       (initial-cluster test)
      :--log-file              pd-logfile
      :--config                pd-configfile)))

(defn start-kv!
  "Starts the TiKV daemon"
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile kv-stdout
       :pidfile kv-pidfile
       :chdir   tidb-dir
       }
      (str "./bin/" kv-bin)
      :--pd             (pd-endpoints test)
      :--addr           (str "0.0.0.0:20160")
      :--advertise-addr (str (name node) ":" "20160")
      :--data-dir       (get-in tidb-map [node :kv])
      :--log-file       kv-logfile
      :--config         kv-configfile)))

(defn start-db!
  "Starts the TiDB daemon"
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile db-stdout
       :pidfile db-pidfile
       :chdir   tidb-dir
       }
      (str "./bin/" db-bin)
      :--store     (str "tikv")
      :--path      (pd-endpoints test)
      :--log-file  db-logfile)))

(defn start!
  "Starts all daemons."
  [test node]
  (start-pd! test node)
  (start-kv! test node)
  (start-db! test node))

(defn stop!
  "Stops all daemons"
  [test node]
  (cu/stop-daemon! db-bin db-pidfile)
  (cu/stop-daemon! kv-bin kv-pidfile)
  (cu/stop-daemon! pd-bin pd-pidfile))

(defn db
  "TiDB"
  [opts]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing TiDB")
        (cu/install-archive! (:tarball test) tidb-dir)

        (configure!)

        (start-pd! test node)
        (Thread/sleep 40000)
        (jepsen/synchronize test)

        ;(start-kv! test node)
        ;(Thread/sleep 20000)
        ;(jepsen/synchronize test)

        ;(start-db! test node)
        ;(sql/await-node node)))
        ))

    (teardown! [_ test node]
      (c/su
        (info node "tearing down TiDB")
        (stop! test node)
        (c/exec :rm :-rf tidb-dir)))

    db/LogFiles
    (log-files [_ test node]
      [db-logfile
       db-stdout
       kv-logfile
       kv-stdout
       pd-logfile
       pd-stdout])))
