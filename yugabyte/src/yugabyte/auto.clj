(ns yugabyte.auto
  "Shared automation functions for configuring, starting and stopping nodes.
  Comes in two flavors: one for the community edition, and one for the
  enterprise edition."
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clojurewerkz.cassaforte.client :as cassandra]
            [clj-http.client :as http]
            [dom-top.core :as dt]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util :refer [meh timeout]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os [debian :as debian]
                       [centos :as centos]]
            [yugabyte.client :as yc])
  (:import jepsen.os.debian.Debian
           jepsen.os.centos.CentOS))

(def dir
  "Where we unpack the Yugabyte package"
  "/home/yugabyte")

(def master-log-dir  (str dir "/master/logs"))
(def tserver-log-dir (str dir "/tserver/logs"))
(def tserver-conf    (str dir "/tserver/conf/server.conf"))
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

; We're going to have two variants of the DB: one for the community edition,
; and one for the enterprise edition. They have different data paths and
; binaries, but share *some* logic, so we're going to have some polymorphic
; functions here for the specific bits they do differently. DBs will implement
; this protocol directly.
(defprotocol Auto
  (install!       [db test])
  (start-master!  [db test node])
  (start-tserver! [db test node])
  (stop-master!   [db])
  (stop-tserver!  [db])
  (wipe!          [db]))

(defn master-nodes
  "Given a test, returns the nodes we run masters on."
  [test]
  ; Right now you must run the same number of masters as --replication-factor.
  ; Maybe? YB's not sure.
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
        #"Timed out"                              (retry (dec tries))
        #"Leader not yet ready to serve requests" (retry (dec tries))
        (throw e)))))

(defn await-tservers
  "Waits until all tservers for a test are online, according to this node."
  [test]
  (dt/with-retry [tries 20]
    (when (< 0 tries 20)
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

(defn start! [db test node]
  "Start both master and tserver. Only starts master if this node is a master
  node. Waits for masters and tservers."
  (when (master-node? test node)
    (start-master! db test node)
    (await-masters test))

  (start-tserver! db test node)
  (await-tservers test)

  (yc/await-setup node)
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
  (meh (c/exec :pkill :--signal signal process-name))
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

(defn wait-for-recovery
  "Waits for the driver to report all nodes are up"
  [timeout-secs conn]
  (timeout (* 1000 timeout-secs)
           (throw (RuntimeException.
                    (str "Driver didn't report all nodes were up in "
                         timeout-secs "s - failing")))
           (while (->> (cassandra/get-hosts conn)
                       (map :is-up) and not)
             (Thread/sleep 500))))

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

(defn get-ce-url
  "Returns URL to community edition tarball for specific released version"
  [version]
  (str "https://downloads.yugabyte.com/yugabyte-ce-" version "-linux.tar.gz"))

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
   ;:--follower_unavailable_considered_failed_sec 10)

   ; Disable YugaByte call-home analytics
   :--callhome_enabled=false
   ])

(def experimental-tuning-flags
  ; Speed up recovery from partitions and crashes. Right now it looks like
  ; these actually make the cluster slower to, or unable to, recover.
  [:--client_read_write_timeout_ms                2000
   :--leader_failure_max_missed_heartbeat_periods 2
   :--leader_failure_exp_backoff_max_delta_ms     5000
   :--rpc_default_keepalive_time_ms               5000
   :--rpc_connection_timeout_ms                   1500
   ])

(defn community-edition
  "Constructs a DB for installing and running the community edition"
  []
  (reify
    Auto
    (install! [db test]
      (c/su
        (c/cd dir
              ; Post-install takes forever, so let's try and skip this on
              ; subsequent runs
              (let [url (or (:url test) (get-ce-url (:version test)))
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

                        (c/exec :echo url (c/lit (str ">>" installed-url-file)))
                        (info "Done with setup")))))))

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
              :--master_addresses   (master-addresses test)
              :--replication_factor (:replication-factor test))))

    (start-tserver! [db test node]
      (c/su (c/exec :mkdir :-p ce-tserver-log-dir)
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
      (c/su (cu/stop-daemon! ce-tserver-bin ce-tserver-pidfile)))

    (wipe! [db]
      (c/su (c/exec :rm :-rf ce-data-dir)))

    db/DB
    (setup! [db test node]
      (install! db test)
      (start! db test node))

    (teardown! [db test node]
      (stop! db test node)
      (wipe! db))

    db/LogFiles
    (log-files [_ _ _]
      (concat [ce-master-logfile
               ce-tserver-logfile]
              (log-files-without-symlinks ce-master-log-dir)
              (log-files-without-symlinks ce-tserver-log-dir)))))

(defn enterprise-edition
  "Enterprise edition of YugabyteDB. Relies on EE already being installed."
  []
  (reify
    Auto
    (install! [db test]
      ; We assume the DB is already installed
      )

    (start-master! [db test node]
      (info "Starting master")
      (info (c/exec (c/lit "if [[ -e /home/yugabyte/master/master.out ]]; then /home/yugabyte/bin/yb-server-ctl.sh master start; fi"))))

    (start-tserver! [db test node]
      (info "Starting tserver")
      (info (c/exec (c/lit "/home/yugabyte/bin/yb-server-ctl.sh tserver start"))))

    (stop-master! [db]
      (info "Stopping master")
      (info (meh (c/exec (c/lit "if [[ -e /home/yugabyte/master/master.out ]]; then /home/yugabyte/bin/yb-server-ctl.sh master stop; sleep 1; pkill -9 yb-master || true; fi")))))

    (stop-tserver! [db]
      (info "Stopping tserver")
      (info (meh (c/exec (c/lit "/home/yugabyte/bin/yb-server-ctl.sh tserver stop; sleep 1; pkill -9 yb-tserver || true")))))

    (wipe! [db]
      (info "Deleting data and log files")
      (meh (c/exec :rm :-r (c/lit (str "/mnt/d*/yb-data/master/*"))))
      (meh (c/exec :rm :-r (c/lit (str "/mnt/d*/yb-data/tserver/*"))))
      (meh (c/exec :mkdir "/mnt/d0/yb-data/master/logs"))
      (meh (c/exec :mkdir "/mnt/d0/yb-data/tserver/logs"))
      (meh (c/exec :sed :-i "/--placement_uuid/d" tserver-conf)))

    db/DB
    (setup! [this test node]
      (install! this test)

      (c/exec :sed :-i "/--max_clock_skew_usec/d" tserver-conf)
      (let [max-skew-ms (test :max-clock-skew-ms)]
        (if (some? max-skew-ms)
          (c/exec :echo (str "--max_clock_skew_usec="
                             (->> (:max-clock-skew-ms test) (* 1000) (* 2) (* max-bump-time-ops-per-test)))
                  (c/lit ">>") tserver-conf)
          (do
            ; Sync clocks on all servers since we are not testing clock skew. Try to stop ntpd, because it won't
            ; let ntpdate to sync clocks.
            (try (c/su (c/exec :service :ntpd :stop))
                 (catch RuntimeException e))
            (c/su (c/exec :ntpdate :-b "pool.ntp.org")))))
      (start! this test node))

    (teardown! [this test node]
      (info "Tearing down YugaByteDB...")
      (stop! this test node)
      (wipe! this))

    db/LogFiles
    (log-files [_ test node]
      (concat
        ; Filter out symlinks.
        (when (master-node? test node)
          (try (remove (partial re-matches #".*\/yb-master.(INFO|WARNING|ERROR)")
                       (cu/ls-full master-log-dir))
               (catch RuntimeException e [])))
        (try (remove (partial re-matches #".*\/yb-tserver.(INFO|WARNING|ERROR)")
                     (cu/ls-full tserver-log-dir))
             (catch RuntimeException e []))
        [tserver-conf]))))

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
