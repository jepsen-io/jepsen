(ns tidb.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
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
(def kv-config-file (str tidb-dir "/kv.conf"))
(def kv-log-file    (str tidb-dir "/kv.log"))
(def kv-stdout      (str tidb-dir "/kv.stdout"))
(def kv-pid-file    (str tidb-dir "/kv.pid"))
(def kv-data-dir    (str tidb-dir "/data/kv"))
(def db-config-file (str tidb-dir "/db.conf"))
(def db-log-file    (str tidb-dir "/db.log"))
(def db-stdout      (str tidb-dir "/db.stdout"))
(def db-pid-file    (str tidb-dir "/db.pid"))

(def client-port 2379)
(def peer-port   2380)

(defn tidb-map
  "Computes node IDs for a test."
  [test]
  (->> (:nodes test)
       (map-indexed (fn [i node]
                      [node {:pd (str "pd" (inc i))
                             :kv (str "kv" (inc i))}]))
       (into {})))

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
       (map (fn [node] (str (get-in (tidb-map test) [node :pd])
                            "=" (peer-url node))))
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
  (c/su (c/exec :echo (slurp (io/resource "pd.conf")) :> pd-config-file)))

(defn configure-kv!
  "Writes configuration file for tikv"
  []
  (c/su (c/exec :echo (slurp (io/resource "tikv.conf")) :> kv-config-file)))

(defn configure-db!
  "Writes configuration file for tidb"
  []
  (c/su (c/exec :echo (slurp (io/resource "tidb.conf")) :> db-config-file)))

(defn configure!
  "Write all config files."
  []
  (configure-pd!)
  (configure-kv!)
  (configure-db!))

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
      :--name                  (get-in (tidb-map test) [node :pd])
      :--data-dir              pd-data-dir
      :--client-urls           (str "http://0.0.0.0:" client-port)
      :--peer-urls             (str "http://0.0.0.0:" peer-port)
      :--advertise-client-urls (client-url node)
      :--advertise-peer-urls   (peer-url node)
      :--initial-cluster       (initial-cluster test)
      :--log-file              pd-log-file
      :--config                pd-config-file)))

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
      :--config         kv-config-file)))

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
      :--config    db-config-file
      :--log-file  db-log-file)))

(defn wait-page
  "Waits for a status page to become available"
  [url]
  (with-retry [delay 1000
               tries 10] ; 1024 seconds
    (c/exec :curl :--fail url)
    (catch Throwable e
      (info "Waiting for page" url)
      (if (pos? tries)
          (do (Thread/sleep delay)
              (retry (* 2 delay) (dec tries)))
          (throw+ {:type  :wait-page
                   :url   url})))))

(defn start!
  "Starts all daemons, waiting for each one's health page in turn."
  [test node]
  (start-pd! test node)
  (wait-page (str "http://127.0.0.1:" client-port "/health"))
  (start-kv! test node)
  (wait-page "http://127.0.0.1:20180/status")
  (start-db! test node)
  (wait-page "http://127.0.0.1:10080/status"))

(defn stop-pd! [test node] (c/su (cu/stop-daemon! pd-bin pd-pid-file)))
(defn stop-kv! [test node] (c/su (cu/stop-daemon! kv-bin kv-pid-file)))
(defn stop-db! [test node] (c/su (cu/stop-daemon! db-bin db-pid-file)))

(defn stop!
  "Stops all daemons"
  [test node]
  (stop-db! test node)
  (stop-kv! test node)
  (stop-pd! test node))

(defn tarball-url
  "Constructs the URL for a tarball; either passing through the test's URL, or
  constructing one from the version."
  [test]
  (or (:tarball-url test)
      (str "http://download.pingcap.org/tidb-" (:version test)
           "-linux-amd64.tar.gz")))

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
      (info (tarball-url test))
      (cu/install-archive! (tarball-url test) tidb-dir)
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

        (start! test node)

        (sql/await-node node)))

    (teardown! [_ test node]
      (c/su
        (info node "tearing down TiDB")
        (stop! test node)
        ; Delete everything but bin/
        (try+ (->> (cu/ls tidb-dir)
                   (remove #{"bin"})
                   (map (partial str tidb-dir "/"))
                   (c/exec :rm :-rf))
              (catch [:type :jepsen.control/nonzero-exit, :exit 2] e
                ; No such dir
                nil))))

    db/LogFiles
    (log-files [_ test node]
      [db-log-file
       db-stdout
       kv-log-file
       kv-stdout
       pd-log-file
       pd-stdout])))
