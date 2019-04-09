(ns tidb.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [dom-top.core :refer [with-retry]]
            [jepsen
              [core :as jepsen]
              [control :as c]
              [db :as db]]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [try+ throw+]]
            [tidb.sql :as sql]))

(def tidb-dir       "/opt/tidb")
(def pd-bin         "pd-server")
(def kv-bin         "tikv-server")
(def db-bin         "tidb-server")
(def pd-config-file (str tidb-dir "/pd.conf"))
(def pd-log-file    (str tidb-dir "/pd.log"))
(def pd-stdout      (str tidb-dir "/pd.stdout"))
(def pd-pid-file    (str tidb-dir "/pd.pid"))
(def pd-data-dir    (str tidb-dir "/data/pd"))
(def kv-config-file (str tidb-dir "/tikv.conf"))
(def kv-log-file    (str tidb-dir "/kv.log"))
(def kv-stdout      (str tidb-dir "/kv.stdout"))
(def kv-pid-file    (str tidb-dir "/kv.pid"))
(def kv-data-dir    (str tidb-dir "/data/kv"))
(def db-log-file    (str tidb-dir "/db.log"))
(def db-stdout      (str tidb-dir "/db.stdout"))
(def db-pid-file    (str tidb-dir "/db.pid"))

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
    (c/exec :echo "[replication]\nmax-replicas=5" :> pd-config-file)))

(defn configure-kv!
  "Writes configuration file for tikv"
  []
  (c/su
    (c/exec :echo "[server]\nstatus-addr=\"0.0.0.0:20180\"\n[raftstore]\npd-heartbeat-tick-interval=\"5s\"\nraft_store_max_leader_lease=\"900ms\"\nraft_base_tick_interval=\"100ms\"\nraft_heartbeat_ticks=3\nraft_election_timeout_ticks=10" :> kv-config-file)))

(defn configure!
  "Write all config files."
  []
  (configure-pd!)
  (configure-kv!))

(defn wait-page
  "Waits a status page available"
  [url]
  (with-retry [tries 20]
    (c/exec :curl :--fail url)
    (catch Throwable e
      (info "Waiting page" url)
      (if (pos? tries)
          (do (Thread/sleep 1000)
              (retry (dec tries)))
          (throw+ {:type "wait-page"
                  :url url})))))

(defn start-pd!
  "Starts the placement driver daemon"
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile pd-stdout
       :pidfile pd-pid-file
       :chdir   tidb-dir
       }
      (str "./bin/" pd-bin)
      :--name                  (get-in tidb-map [node :pd])
      :--data-dir              pd-data-dir
      :--client-urls           (str "http://0.0.0.0:" client-port)
      :--peer-urls             (str "http://0.0.0.0:" peer-port)
      :--advertise-client-urls (client-url node)
      :--advertise-peer-urls   (peer-url node)
      :--initial-cluster       (initial-cluster test)
      :--log-file              pd-log-file
      :--config                pd-config-file))
  (wait-page (str "http://127.0.0.1:" client-port "/health")))

(defn start-kv!
  "Starts the TiKV daemon"
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile kv-stdout
       :pidfile kv-pid-file
       :chdir   tidb-dir
       }
      (str "./bin/" kv-bin)
      :--pd             (pd-endpoints test)
      :--addr           (str "0.0.0.0:20160")
      :--advertise-addr (str (name node) ":" "20160")
      :--data-dir       kv-data-dir
      :--log-file       kv-log-file
      :--config         kv-config-file))
  (wait-page "http://127.0.0.1:20180/status"))

(defn start-db!
  "Starts the TiDB daemon"
  [test node]
  (c/su
    (cu/start-daemon!
      {:logfile db-stdout
       :pidfile db-pid-file
       :chdir   tidb-dir
       }
      (str "./bin/" db-bin)
      :--store     (str "tikv")
      :--path      (pd-endpoints test)
      :--log-file  db-log-file))
  (wait-page "http://127.0.0.1:10080/status"))

(defn start!
  "Starts all daemons."
  [test node]
  (start-pd! test node)
  (start-kv! test node)
  (start-db! test node))

(defn stop-pd! [test node] (cu/stop-daemon! pd-bin pd-pid-file))
(defn stop-kv! [test node] (cu/stop-daemon! kv-bin kv-pid-file))
(defn stop-db! [test node] (cu/stop-daemon! db-bin db-pid-file))

(defn stop!
  "Stops all daemons"
  [test node]
  (stop-db! test node)
  (stop-kv! test node)
  (stop-pd! test node))

(defn install!
  "Downloads archive and extracts it to our local tidb-dir, if it doesn't exist
  already. If test contains a :force-reinstall key, we always install a fresh
  copy.

  Calls `sync`; this tarball is *massive* (1.1G), and when we start tidb, it'll
  try to fsync, and cause, like, 60s stalls on single nodes, wrecking the
  cluster."
  [test node]
  (c/su
    (when (or (:force-reinstall test) (not (cu/exists? tidb-dir)))
      (info node "installing TiDB")
      (cu/install-archive! (:tarball test) tidb-dir)
      (info "Syncing disks to avoid slow fsync on db start")
      (c/exec :sync))))

(defn db
  "TiDB"
  []
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (install! test node)
        (configure!)

        (start-pd! test node)
        (start-kv! test node)
        (start-db! test node)

        (sql/await-node node)))

    (teardown! [_ test node]
      (c/su
        (info node "tearing down TiDB")
        (stop! test node)
        ; Delete everything but bin/
        (->> (cu/ls tidb-dir)
             (remove #{"bin"})
             (map (partial str tidb-dir "/"))
             (c/exec :rm :-rf))))

    db/LogFiles
    (log-files [_ test node]
      [db-log-file
       db-stdout
       kv-log-file
       kv-stdout
       pd-log-file
       pd-stdout])))
