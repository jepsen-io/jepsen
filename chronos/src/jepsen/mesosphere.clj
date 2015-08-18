(ns jepsen.mesosphere
  "Sets up nodes as a mesosphere cluster."
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen [client :as client]
                    [db :as db]
                    [tests :as tests]
                    [control :as c :refer [|]]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util :refer [meh timeout]]
                    [zookeeper :as zk]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def master-count "How many master nodes should we run?" 3)
(def master-pidfile "/var/run/mesos/master.pid")
(def slave-pidfile "/var/run/mesos/slave.pid")
(def master-dir    "/var/lib/mesos/master")
(def slave-dir     "/var/lib/mesos/slave")
(def log-dir       "/var/log/mesos")
(def master-bin    "/usr/sbin/mesos-master")
(def slave-bin     "/usr/sbin/mesos-slave")

(defn install!
  [test node version]
  (debian/add-repo! :mesosphere
                    "deb http://repos.mesosphere.io/debian wheezy main"
                    "keyserver.ubuntu.com"
                    "E56151BF")
  (debian/install {:mesos version})
  (c/su
    (c/exec :mkdir :-p "/var/run/mesos")
    (c/exec :mkdir :-p master-dir)
    (c/exec :mkdir :-p slave-dir)))

(defn zk-uri
  "Zookeeper connection URI for a test"
  [test]
  (str "zk://"
       (->> test
            :nodes
            (map (fn [node]
                   (str (name node) ":2181")))
            (str/join ","))
       "/mesos"))

(defn configure!
  "We don't use these for starting the server because it doesn't ship with init
  scripts for some reason, but I think chronos looks at them."
  [test]
  (c/su
    (c/exec :echo (zk-uri test)
            :> "/etc/mesos/zk")

    (c/exec :echo (util/majority master-count)
            :> "/etc/mesos-master/quorum")))

(defn start-master!
  "Starts the master (when this node is one of the nodes in the test which is
  eligible to be a master).

  Fighting a losing battle between problematic terminology and a clear mapping
  of code to DB concepts here; maybe change this back to \"primary\" or
  \"supervisor\" later?"
  [test node]
  (when (some #{node} (take master-count (sort (:nodes test))))
    (info node "starting mesos-master")
    (c/su
      (c/exec :start-stop-daemon :--start
              :--background
              :--make-pidfile
              :--pidfile        master-pidfile
              :--chdir          master-dir
              :--no-close
              :--oknodo
              :--exec           "/usr/bin/env"
              :--
              "GLOG_v=1"
              master-bin
              (str "--hostname="  (name node))
              (str "--log_dir="   log-dir)
              (str "--quorum="    (util/majority master-count))
              (str "--registry_fetch_timeout=120secs")
              (str "--registry_store_timeout=5secs")
              (str "--work_dir="  master-dir)
              (str "--offer_timeout=30secs")
              (str "--zk="        (zk-uri test))
              :>> (str log-dir "/master.stdout")
              (c/lit "2>&1")))))

(defn start-slave!
  "Starts the slave (when this node is one of the nodes in the test which is
  eligible to be a slave).

  Fighting a losing battle between problematic terminology and a clear mapping
  of code to DB concepts here; maybe change this back to \"primary\" or
  \"supervisor\" later?"
  [test node]
  (when-not (some #{node} (take master-count (sort (:nodes test))))
    (info node "starting mesos-slave")
    (c/su
      (c/exec :start-stop-daemon :--start
              :--background
              :--make-pidfile
              :--pidfile        slave-pidfile
              :--chdir          slave-dir
              :--exec           slave-bin
              :--no-close
              :--oknodo
              :--
              (str "--hostname="  (name node))
              (str "--log_dir="   log-dir)
              (str "--recovery_timeout=30secs")
              (str "--work_dir="  slave-dir)
              (str "--master="    (zk-uri test))
              :>> (str log-dir "/slave.stdout")
              (c/lit "2>&1")))))

(defn stop-master!
  [node]
  (info node "stopping mesos-master")
  (meh (c/exec :killall :-9 :mesos-master))
  (meh (c/exec :rm :-rf master-pidfile)))

(defn stop-slave!
  [node]
  (info node "stopping mesos-slave")
  (meh (c/exec :killall :-9 :mesos-slave))
  (meh (c/exec :rm :-rf slave-pidfile)))

(defn db
  "Installs mesos. You can get versions from

      curl http://repos.mesosphere.com/ubuntu/dists/precise/main/binary-amd64/Packages.bz2 | bunzip2 | egrep '^Package:|^Version:' | paste - - | sort"
  [version]
  (let [zk (zk/db "3.4.5+dfsg-2")]
    (reify db/DB
      (setup! [_ test node]
        (db/setup! zk test node)
        (install! test node version)
        (configure! test)
        (start-master! test node)
        (start-slave! test node))

      (teardown! [_ test node]
        (stop-slave! node)
        (stop-master! node)
        (c/su (c/exec :rm :-rf
                      (c/lit (str master-dir "/*"))
                      (c/lit (str slave-dir "/*"))
                      (c/lit (str log-dir "/*"))))
        (db/teardown! zk test node))

      db/LogFiles
      (log-files [_ test node]
        (concat (db/log-files zk test node)
                (cu/ls-full log-dir))))))
