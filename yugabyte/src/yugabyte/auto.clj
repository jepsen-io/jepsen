(ns yugabyte.auto
  "Shared automation functions for configuring, starting and stopping nodes."
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clj-http.client :as http]
            [dom-top.core :as dt]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.util :as util :refer [meh timeout]]
            [jepsen.control.net :as cn]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.os.centos :as centos]
            [yugabyte.ycql.client :as ycql.client]
            [yugabyte.ysql.client :as ysql.client]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import jepsen.os.debian.Debian
           jepsen.os.centos.CentOS))

(def dir
  "Where we unpack the Yugabyte package"
  "/home/yugabyte")

(def master-log-dir  (str dir "/master/logs"))
(def tserver-log-dir (str dir "/tserver/logs"))
(def installed-url-file (str dir "/installed-url"))

(def max-bump-time-ops-per-test
  "Upper bound on number of bump time ops per test, needed to estimate max
  clock skew between servers"
  100)

; OS-level polymorphic functions for Yugabyte
(defprotocol OS
  (install-python! [os]))

(extend-protocol OS
  Debian
  (install-python! [os]
    (debian/install [:python2.7]))

  CentOS
  (install-python! [os]
    ; TODO: figure out the yum invocation we need here
    ))

(defprotocol Auto
  (install!       [db test])
  (configure!     [db test node])
  (start-master!  [db test node])
  (start-tserver! [db test node])
  (stop-master!   [db])
  (stop-tserver!  [db])
  (wipe!          [db]))

(defn master-nodes
  "Given a test, returns the nodes we run masters on."
  [test]
  (let [nodes (take (:replication-factor test)
                    (:nodes test))]
    (assert (= (count nodes) (:replication-factor test))
            (str "We need at least "
                 (:replication-factor test)
                 " nodes as masters, but test only has nodes: "
                 (pr-str (:nodes test))))
    nodes))

(defn master-node?
  "Is this node a master?"
  [test node]
  (some #{node} (master-nodes test)))

(defn master-addresses
  "Given a test, returns a list of master addresses, like \"n1:7100,n2:7100,...
  \""
  [test]
  (assert (coll? (:nodes test)))
  (->> (master-nodes test)
       (take (:replication-factor test))
       (map #(str % ":7100"))
       (str/join ",")))

(defn yb-admin
  "Runs a yb-admin command on a node. Args are passed to yb-admin."
  [test & args]
  (apply c/exec (str dir "/bin/yb-admin")
         :--master_addresses (master-addresses test)
         args))

(defn list-all-masters
  "Asks a node to list all the masters it knows about."
  [test]
  (->> (yb-admin test :list_all_masters)
       (str/split-lines)
       rest
       (map (fn [line]
              (->> line
                   (re-find #"(\w+)\s+([^\s]+)\s+(\w+)\s+(\w+)")
                   next
                   (zipmap [:uuid :address :state :role]))))))

(defn list-all-tservers
  "Asks a node to list all the tservers it knows about."
  [test]
  (->> (yb-admin test :list_all_tablet_servers)
       (str/split-lines)
       rest
       (map (fn [line]
              (->> line
                   (re-find #"(\w+)\s+([^\s]+)")
                   next
                   (zipmap [:uuid :address]))))))

(defn await-masters
  "Waits until all masters for a test are online, according to this node."
  [test]
  (dt/with-retry [tries 20]
    (when (< 0 tries 20)
      (info "Waiting for masters to come online")
      (Thread/sleep 1000))

    (when (zero? tries)
      (throw (RuntimeException. "Giving up waiting for masters.")))

    (when-not (= (count (master-addresses test))
                 (->> (list-all-masters test)
                      (filter (comp #{"ALIVE"} :state))
                      count))
      (retry (dec tries)))

    :ready

    (catch RuntimeException e
      (condp re-find (.getMessage e)
        #"Could not locate the leader master"     (retry (dec tries))
        #"Timed out"                              (retry (dec tries))
        #"Leader not yet ready to serve requests" (retry (dec tries))
        (throw e)))))

(defn await-tservers
  "Waits until all tservers for a test are online, according to this node."
  [test]
  (dt/with-retry [tries 60]
    (when (< 0 tries)
      (info "Waiting for tservers to come online")
      (Thread/sleep 1000))

    (when (zero? tries)
      (throw (RuntimeException. "Giving up waiting for tservers.")))

    (when-not (= (count (:nodes test))
                 (->> (list-all-tservers test)
                      (filter (comp #{"ALIVE"} :state))
                      count))
      (retry (dec tries)))

    :ready

    (catch RuntimeException e
      (condp re-find (.getMessage e)
        #"Leader not yet ready to serve requests"   (retry (dec tries))
        #"This leader has not yet acquired a lease" (retry (dec tries))
        #"Could not locate the leader master"       (retry (dec tries))
        #"Leader not yet replicated NoOp"           (retry (dec tries))
        #"Not the leader"                           (retry (dec tries))
        (throw e)))))

(defn check-ysql
  "Connects to the YSQL interface and immediately disconnects. YB just...
  doesn't accept connections sometimes, so we use this to give up on the setup
  process if the cluster looks broken. Hack hack hack."
  [node]
  (try+
    (-> node
        ysql.client/open-conn
        ysql.client/close-conn)
    (catch [:type :connection-timed-out] e
      (throw+ {:type :jepsen.db/setup-failed}))))

(defn start! [db test node]
  "Start both master and tserver. Only starts master if this node is a master
  node. Waits for masters and tservers."
  (info "Starting master and tserver for" (name (:api test)) "API")
  (when (master-node? test node)
    (start-master! db test node)
    (await-masters test))

  (start-tserver! db test node)
  (await-tservers test)

  (if (= (:api test) :ycql)
    (yugabyte.ycql.client/await-setup node)
    ()) ; So far it looks like we don't need that for YSQL?
  :started)

(defn stop! [db test node]
  "Stop both master and tserver. Only stops master if this node needs to."
  (stop-tserver! db)
  (when (master-node? test node)
    (stop-master! db))
  :stopped)

(defn signal!
  "Sends a signal to a named process by signal number or name."
  [process-name signal]
  (meh (c/su (c/exec :pkill :--signal signal process-name)))
  :signaled)

(defn kill!
  "Kill a process forcibly."
  [process]
  (signal! process 9)
  (c/exec (c/lit (str "! ps -ce | grep " process)))
  (info process "killed")
  :killed)

(defn kill-tserver!
  "Kills the tserver"
  [db]
  (kill! "yb-tserver")
  (stop-tserver! db))

(defn kill-master!
  "Kills the master"
  [db]
  (kill! "yb-master")
  (stop-master! db))

(defn version
  "Returns a map of version information by calling `bin/yb-master --version`,
  including:

      :version
      :build
      :revision
      :build-type
      :timestamp"
  []
  (try
    (-> #"version (.+?) build (.+?) revision (.+?) build_type (.+?) built at (.+)"
        (re-find (c/exec (str dir "/bin/yb-master") :--version))
        next
        (->> (zipmap [:version :build :revision :build-type :timestamp])))
    (catch RuntimeException e
      ; Probably not installed
      )))

(defn get-installed-url
      "Returns URL from which YugaByte was installed on node"
      []
      (try
        (c/exec :cat installed-url-file)
        (catch RuntimeException e
          ; Probably not installed
          )))

(defn get-download-url
  "Returns URL to tarball for specific released version"
  [version]
  (str "https://downloads.yugabyte.com/yugabyte-" version "-linux.tar.gz"))

(defn log-files-without-symlinks
  "Takes a directory, and returns a list of logfiles in that direcory, skipping
  the symlinks which end in .INFO, .WARNING, etc."
  [dir]
  (remove (partial re-find #"\.(INFO|WARNING|ERROR)$")
          (try (cu/ls-full dir)
               (catch RuntimeException e nil))))

; Community-edition-specific files
(def ce-data-dir        (str dir "/data"))

(def ce-master-bin      (str dir "/bin/yb-master"))
(def ce-master-log-dir  (str ce-data-dir "/yb-data/master/logs"))
(def ce-master-logfile  (str ce-master-log-dir "/stdout"))
(def ce-master-pidfile  (str dir "/master.pid"))

(def ce-tserver-bin     (str dir "/bin/yb-tserver"))
(def ce-tserver-log-dir (str ce-data-dir "/yb-data/tserver/logs"))
(def ce-tserver-logfile (str ce-tserver-log-dir "/stdout"))
(def ce-tserver-pidfile (str dir "/tserver.pid"))

(defn ce-shared-opts
  "Shared options for both master and tserver"
  [node]
  [; Data files!
   :--fs_data_dirs         ce-data-dir
   ; Limit memory to 2GB
   :--memory_limit_hard_bytes 2147483648
   ; Fewer shards to improve perf
   :--yb_num_shards_per_tserver 4
   ; YB can do weird things with loopback interfaces, so... bind explicitly
   :--rpc_bind_addresses (cn/ip node)
   ; Seconds before declaring an unavailable node dead and initiating a raft
   ; membership change
   ;:--follower_unavailable_considered_failed_sec 10
   ; Clock skew threshold
   ; :--max_clock_skew_usec 1
   ])

(defn master-api-opts
  "API-specific options for master"
  [api node]
  (if (= api :ysql)
    [:--use_initial_sys_catalog_snapshot]
    []))

(defn tserver-api-opts
  "API-specific options for tserver"
  [api node]
  (if (= api :ysql)
    [:--start_pgsql_proxy
     :--pgsql_proxy_bind_address (cn/ip node)]
    []))

(def experimental-tuning-flags
  ; Speed up recovery from partitions and crashes. Right now it looks like
  ; these actually make the cluster slower to, or unable to, recover.
  [:--client_read_write_timeout_ms                2000
   :--leader_failure_max_missed_heartbeat_periods 2
   :--leader_failure_exp_backoff_max_delta_ms     5000
   :--rpc_default_keepalive_time_ms               5000
   :--rpc_connection_timeout_ms                   1500
   ])

(def limits-conf
  "Ulimits, in the format for /etc/security/limits.conf."
  "
* hard nofile 1048576
* soft nofile 1048576")

(defrecord YugaByteDB
  []
  Auto
  (install! [db test]
    (c/su
      (c/cd dir
            ; Post-install takes forever, so let's try and skip this on
            ; subsequent runs
            (let [url           (or (:url test) (get-download-url (:version test)))
                  installed-url (get-installed-url)]
              (when-not (= url installed-url)
                (info "Replacing version" installed-url "with" url)
                (install-python! (:os test))
                (assert (re-find #"Python 2\.7"
                                 (c/exec :python :--version (c/lit "2>&1"))))

                (info "Installing tarball")
                (cu/install-archive! url dir)
                (c/su (info "Post-install script")
                      (c/exec "./bin/post_install.sh")

                      (c/exec :echo url :>> installed-url-file)
                      (info "Done with setup")))))))

  (configure! [db test node]
    ; YB will explode after creating just a handful of tables if we don't raise
    ; ulimits. This is sort of a hack; it won't take effect for the current
    ; session, but will on the second and subsequent runs. We can't run
    ; `ulimit` directly because the shell context doesn't carry over to
    ; subsequent commands. Should write a subshell exec thing to handle this at
    ; some point.
    (c/su (c/exec :echo limits-conf :> "/etc/security/limits.d/jepsen.conf")))

  (start-master! [db test node]
    (c/su (c/exec :mkdir :-p ce-master-log-dir)
          (cu/start-daemon!
            {:logfile ce-master-logfile
             :pidfile ce-master-pidfile
             :chdir   dir}
            ce-master-bin
            (ce-shared-opts node)
            (when (:experimental-tuning-flags test)
              experimental-tuning-flags)
            :--master_addresses (master-addresses test)
            :--replication_factor (:replication-factor test)
            (master-api-opts (:api test) node)
            )))

  (start-tserver! [db test node]
    (c/su (info "ulimit\n" (c/exec :ulimit :-a))
          (c/exec :mkdir :-p ce-tserver-log-dir)
          (cu/start-daemon!
            {:logfile ce-tserver-logfile
             :pidfile ce-tserver-pidfile
             :chdir   dir}
            ce-tserver-bin
            (ce-shared-opts node)
            (when (:experimental-tuning-flags test)
              experimental-tuning-flags)
            :--tserver_master_addrs (master-addresses test)
            ; Tracing
            :--enable_tracing
            :--rpc_slow_query_threshold_ms 1000
            :--load_balancer_max_concurrent_adds 10
            (tserver-api-opts (:api test) node)

            ; Heartbeats
            ;:--heartbeat_interval_ms 100
            ;:--heartbeat_rpc_timeout_ms 1500
            ;:--retryable_rpc_single_call_timeout_ms 2000
            ;:--rpc_connection_timeout_ms 1500
            ;:--leader_failure_exp_backoff_max_delta_ms 1000
            ;:--leader_failure_max_missed_heartbeat_period 3
            ;:--consensus_rpc_timeout_ms 300
            ;:--client_read_write_timeout_ms 6000
            )))

  (stop-master! [db]
    (c/su (cu/stop-daemon! ce-master-bin ce-master-pidfile)))

  (stop-tserver! [db]
    (c/su (cu/stop-daemon! ce-tserver-bin ce-tserver-pidfile))
    (c/su (cu/grepkill! "postgres")))

  (wipe! [db]
    (c/su (c/exec :rm :-rf ce-data-dir)))

  db/DB
  (setup! [db test node]
    (install! db test)
    (configure! db test node)
    (start! db test node)
    (check-ysql node))

  (teardown! [db test node]
    (stop! db test node)
    (wipe! db))

  db/Primary
  (setup-primary! [this test node]
    "Executed once on a first node in list (i.e. n1 by default) after per-node setup is done"
    ; NOOP placeholder, can be used to initialize cluster for different APIs
    )

  db/LogFiles
  (log-files [_ _ _]
    (concat [ce-master-logfile
             ce-tserver-logfile]
            (log-files-without-symlinks ce-master-log-dir)
            (log-files-without-symlinks ce-tserver-log-dir))))

(defn running-masters
  "Returns a list of nodes where master process is running."
  [nodes]
  (->> nodes
       (pmap (fn [node]
               (try
                 (let [is-running
                       (-> (str "http://" node ":7000/jsonmetricz")
                           (http/get)
                           :status
                           (= 200))]
                   [node is-running])
                 (catch Exception e [node false])
                 )))
       (filter second)
       (map first)))
