(ns tidb.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [dom-top.core :refer [with-retry]]
            [fipp.edn :refer [pprint]]
            [jepsen [core :as jepsen]
                    [control :as c]
                    [db :as db]
                    [faketime :as faketime]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [try+ throw+]]
            [tidb.sql :as sql]))

(def replica-count
  "This should probably be used to generate max-replicas in pd.conf as well,
  but for now we'll just write it in both places."
  3)

(def tidb-dir       "/opt/tidb")
(def tidb-bin-dir   "/opt/tidb/bin")
(def pd-bin         "pd-server")
(def pdctl-bin      "pd-ctl")
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
(def db-slow-file   (str tidb-dir "/slow.log"))
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

(defn pd-api-path
  "Constructs an API path for the PD located on the given node."
  [node & path-components]
  (str "http://" node ":2379/pd/api/v1/" (str/join "/" path-components)))

(defn pd-members
  "All members of the cluster"
  [node]
  (-> (pd-api-path node "members")
      (http/get {:as :json})
      :body))

(defn pd-leader
  "Gets the current PD leader."
  [node]
  (-> (pd-api-path node "leader")
      (http/get {:as :json})
      :body))

(defn pd-leader-node
  "Returns the name of the node which is the current PD leader."
  [test node]
  (let [leader-name (:name (pd-leader node))]
    (->> (tidb-map test)
         (keep (fn [[node m]]
                 (when (= leader-name (:pd m))
                   node)))
         first)))

(defn pd-transfer-leader!
  "Transfer leadership to the given leader map."
  [node next-leader]
  (assert (map? next-leader))
  (-> (pd-api-path node "leader" "transfer" (:name next-leader))
      (http/post)))

(defn pd-regions
  "All PD regions"
  [node]
  (-> (pd-api-path node "regions")
      (http/get {:as :json})
      :body))

(defmacro await-http
  "Loops body until HTTP call returns, retrying 500 and 503 errors."
  [& body]
  `(loop []
     (let [res# (try+ ~@body
                      (catch [:status 500] _# ::retry)
                      (catch [:status 503] _# ::retry))]
       (if (= ::retry res#)
         (recur)
         res#))))

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

(defn page-ready?
  "Fetches a status page URL on the local node, and returns true iff the page
  was available."
  [url]
  (try+
    (c/exec :curl :--fail url)
    (catch [:type :jepsen.control/nonzero-exit] _ false)))

(defn pd-ready?
  "Is Placement Driver ready?"
  []
  (page-ready? (str "http://127.0.0.1:" client-port "/health")))

(defn kv-ready?
  "Is TiKV ready?"
  []
  (page-ready? "http://127.0.0.1:20180/status"))

(defn db-ready?
  "Is TiDB ready?"
  []
  (page-ready? "http://127.0.0.1:10080/status"))

(defn restart-loop*
  "TiDB is fragile on startup; processes love to crash if they can't complete
  their initial requests to network dependencies. We try to work around this by
  checking whether the daemon is running, and restarting it if necessary.

  Takes a name (used for error messages and logging), a function which starts
  the node, and a status function which returns one of three states:

  :starting - The node is still starting up
  :ready    - The node is (hopefully) ready to serve requests
  :crashed  - The node crashed

  We call start, then poll the status function until we see :ready or :crashed.
  If ready, returns. If :crashed, restarts the node and tries again."
  [name start! get-status]
  (let [deadline (+ (util/linear-time-nanos) (util/secs->nanos 300))]
    ; First startup!
    (start!)

    (loop [status :init]
      (when (< deadline (util/linear-time-nanos))
        ; Out of time
        (throw+ {:type :restart-loop-timed-out, :service name}))

      (when (= status :crashed)
        ; Need to restart
        (info name "crashed during startup; restarting")
        (start!))

      ; Give it a bit
      (Thread/sleep 1000)

      ; OK, how's it doing?
      (let [status (get-status)]
        (if (= status :ready)
          ; Done
          status
          ; Still working
          (recur status))))))

(defmacro restart-loop
  "Macro form of restart-loop*: takes two forms instead of two functions."
  [name start! get-status]
  `(restart-loop* ~name
                  (fn ~'start!     [] ~start!)
                  (fn ~'get-status [] ~get-status)))

(defn start-wait-pd!
  "Starts PD, waiting for the health page to come online."
  [test node]
  (restart-loop :pd (start-pd! test node)
                (cond (pd-ready?)                       :ready
                      (cu/daemon-running? pd-pid-file)  :starting
                      true                              :crashed)))

(defn start-wait-kv!
  "Starts TiKV, waiting for the health page to come online."
  [test node]
  (restart-loop :kv (start-kv! test node)
                (cond (kv-ready?)                       :ready
                      (cu/daemon-running? kv-pid-file)  :starting
                      true                              :crashed)))

(defn start-wait-db!
  "Starts TiDB, waiting for the health page to come online."
  [test node]
  (restart-loop :db (start-db! test node)
                (cond (db-ready?)                       :ready
                      (cu/daemon-running? db-pid-file)  :starting
                      true                              :crashed)))

(defn stop-pd! [test node] (c/su
                             ; Faketime wrapper means we only kill the wrapper
                             ; script, not the underlying binary
                             (cu/stop-daemon! pd-bin pd-pid-file)
                             (cu/grepkill! pd-bin)))

(defn stop-kv! [test node] (c/su (cu/stop-daemon! kv-bin kv-pid-file)
                                 (cu/grepkill! kv-bin)))

(defn stop-db! [test node] (c/su (cu/stop-daemon! db-bin db-pid-file)
                                 (cu/grepkill! db-bin)))

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
      (str "http://download.pingcap.org/tidb-v" (:version test)
           "-linux-amd64.tar.gz")))

(defn setup-faketime!
  "Configures the faketime wrapper for this node, so that the given binary runs
  at the given rate."
  [bin rate]
  (info "Configuring" bin "to run at" (str rate "x realtime"))
  (c/su (faketime/wrap! (str tidb-bin-dir "/" bin) 0 rate)))

(defn install!
  "Downloads archive and extracts it to our local tidb-dir, if it doesn't exist
  already. If test contains a :force-reinstall key, we always install a fresh
  copy.

  Calls `sync`; this tarball is *massive* (1.1G), and when we start tidb, it'll
  try to fsync, and cause, like, 60s stalls on single nodes, wrecking the
  cluster."
  [test node]
  (c/su
    (when (or (:force-reinstall test)
              (not (cu/exists? tidb-dir)))
      (info node "installing TiDB")
      (info (tarball-url test))
      (cu/install-archive! (tarball-url test) tidb-dir)
      (info "Syncing disks to avoid slow fsync on db start")
      (c/exec :sync))
    (if-let [ratio (:faketime test)]
      (do ; We need a special fork of faketime specifically for tikv, which
          ; uses CLOCK_MONOTONIC_COARSE (not supported by 0.9.6 stock), and
          ; jemalloc (segfaults on 0.9.7).
          (faketime/install-0.9.6-jepsen1!)
          ; Add faketime wrappers
          (setup-faketime! pd-bin (faketime/rand-factor ratio))
          (setup-faketime! kv-bin (faketime/rand-factor ratio))
          (setup-faketime! db-bin (faketime/rand-factor ratio)))
      (c/cd tidb-bin-dir
            ; Destroy faketime wrappers, if applicable.
            (faketime/unwrap! pd-bin)
            (faketime/unwrap! kv-bin)
            (faketime/unwrap! db-bin)))))

(defn region-ready?
  "Does the given region have enough replicas?"
  [region]
  (->> (:peers region)
       (remove :is_learner)
       count
       (<= replica-count)))

(defn wait-for-replica-count
  "TiDB start with a single replica, and only adds more later. We have to wait
  for it to do that before starting our tests if we want to be able to test
  things like failover. <sigh>"
  [node]
  (loop [tries 1000]
    (when (zero? tries)
      (throw+ {:type :gave-up-waiting-for-replica-count}))

    (let [regions (await-http (pd-regions node))]
      ;(info :regions (with-out-str (pprint regions)))
      (info :region-replicas (->> (:regions regions)
                                  (map (fn [region]
                                         [(:id region)
                                          (->> (:peers region)
                                               (remove :is_learner)
                                               count)]))
                                  (into (sorted-map))
                                  pprint
                                  with-out-str))
      (if (every? region-ready? (:regions regions))
        true
        (do (Thread/sleep 10000)
            (recur (dec tries)))))))

(defn db
  "TiDB"
  []
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (install! test node)
        (configure!)

        (try+ (start-wait-pd! test node)
              ; If we don't synchronize, KV might explode because PD isn't
              ; fully available
              (jepsen/synchronize test)

              (start-wait-kv! test node)
              (jepsen/synchronize test)

              ; We have to wait for every region to become totally replicated
              ; before starting TiDB: if we start TiDB first, it might take 80+
              ; minutes to converge.
              (wait-for-replica-count node)
              (jepsen/synchronize test)

              ; OK, now we can start TiDB itself
              (start-wait-db! test node)

              ; For reasons I cannot explain, sometimes TiDB just... fails to
              ; reach a usable state despite waiting hundreds of seconds to
              ; open a connection. I've lowered the await-node timeout, and if
              ; we fail here, we'll nuke the entire setup process and try
              ; again. <sigh>
              (sql/await-node node)

              (catch [:type :gave-up-waiting-for-replica-count] e
                (throw+ {:type :jepsen.db/setup-failed}))

              (catch [:type :restart-loop-timed-out] e
                (throw+ {:type :jepsen.db/setup-failed}))

              (catch [:type :connect-timed-out] e
                ; sigh
                (throw+ {:type :jepsen.db/setup-failed}))

              (catch java.sql.SQLException e
                ; siiiiiiiigh
                (throw+ {:type :jepsen.db/setup-failed})))))

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
       db-slow-file
       db-stdout
       kv-log-file
       kv-stdout
       pd-log-file
       pd-stdout])))
