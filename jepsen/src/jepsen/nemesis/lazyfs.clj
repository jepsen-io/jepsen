(ns jepsen.nemesis.lazyfs
  "The lazyfs nemesis allows the injection of filesystem-level faults:
  specifically, losing data which was written to disk but not fsynced.

  In containers you may not have /dev/fuse by default; you can create it with

      mknod /dev/fuse c 10 229

  I've opted not to make this automatic here, because it feels like it might be
  risky on strangers' machines."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [util :as util :refer [timeout]]
                    [nemesis :as nemesis]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io File)))

(def repo-url
  "Where can we clone lazyfs from?"
  "git@github.com:dsrhaslab/lazyfs.git")

(def commit
  "What version should we check out and build?"
  "657a007bc764d2509534e1fcbf8345eec8cf4de1")

(def dir
  "Where do we install lazyfs to on the remote node?"
  "/opt/jepsen/lazyfs")

(def bin
  "The lazyfs binary"
  (str dir "/lazyfs/build/lazyfs"))

(def fuse-dev
  "The path to the fuse device."
  "/dev/fuse")

(defn config
  "The lazyfs config file text. Takes a lazyfs map"
  [{:keys [fifo]}]
  (str "[faults]
fifo_path=\"" fifo "\"

[cache]
apply_eviction=false

[cache.simple]
custom_size=\"0.5GB\"
blocks_per_page=1"))

(def real-extension
  "When we mount a lazyfs directory, it's backed by a real directory on the
  underlying filesystem: e.g. 'foo' is backed by 'foo.real'. We name this
  directory using this extension."
  ".real")

(defn install!
  "Installs lazyfs on the currently-bound remote node."
  []
  (info "Installing lazyfs")
  (c/su
    ; Dependencies
    (debian/install [:g++ :cmake :libfuse3-dev :libfuse3-3 :fuse3])
    ; LXC containers like to delete /dev/fuse on reboot for some reason
    (when-not (cu/exists? fuse-dev)
      (c/exec :mknod fuse-dev "c" 10 229))
    ; Set up allow_other so anyone can read and write to the fs
    (c/exec :sed :-i "/\\s*user_allow_other/s/^#//g" "/etc/fuse.conf")
    ; Get the repo
    (when-not (cu/exists? dir)
      (c/exec :mkdir :-p (str/replace dir #"/[^/]+$" ""))
      (try+
        (c/exec :git :clone repo-url dir)
        (catch [:exit 128] _
          ; Host key not known
          (c/exec :ssh-keyscan :-t :rsa "github.com" :>> "~/.ssh/known_hosts")
          (c/exec :git :clone repo-url dir))))
    ; Check out version
    (c/cd dir
          (c/exec :git :fetch)
          (c/exec :git :checkout commit)
          (c/exec :git :clean :-fx)
          ; Install libpcache
          (c/cd "libs/libpcache"
                (c/exec "./build.sh"))
          ; Install lazyfs
          (c/cd "lazyfs"
            (c/exec "./build.sh")))))

(defn lazyfs
  "Constructs a map of all the files we need to run a lazyfs map for a
  directory."
  [dir]
  (let [lazyfs-dir  (str dir ".lazyfs")
        data-dir    (str lazyfs-dir "/data")
        fifo        (str lazyfs-dir "/fifo")
        config-file (str lazyfs-dir "/config")
        log-file    (str lazyfs-dir "/log")
        pid-file    (str lazyfs-dir "/pid")]
    {:dir         dir
     :lazyfs-dir  lazyfs-dir
     :data-dir    data-dir
     :fifo        fifo
     :config-file config-file
     :log-file    log-file
     :pid-file    pid-file}))

(defn start-daemon!
  "Starts the lazyfs daemon once preparation is complete. We daemonize
  ourselves so that we can get logs--also it looks like the built-in daemon
  might not work right now."
  [{:keys [dir lazyfs-dir data-dir pid-file log-file fifo config-file]}]
  (cu/start-daemon!
    {:chdir   lazyfs-dir
     :logfile log-file
     :pidfile pid-file}
    bin
    dir
    :--config-path config-file
    :-o "allow_other"
    :-o "modules=subdir"
    :-o (str "subdir=" data-dir)
    :-f))

(defn mount!
  "Takes a lazyfs map, creates directories and config files, and starts the
  lazyfs daemon. You likely want to call this before beginning database setup.
  Returns the lazyfs map."
  [lazyfs]
  (info "Mounting lazyfs" (:dir lazyfs))
  ; Make directories
  (c/exec :mkdir :-p (:dir lazyfs))
  (c/exec :mkdir :-p (:data-dir lazyfs))
  ; Write config file
  (cu/write-file! (config lazyfs) (:config-file lazyfs))
  ; And go!
  (try+
    (start-daemon! lazyfs)
    (catch [:exit -1] e
      (if (re-find #"fuse: device not found" (:err e))
        (do (c/exec :mknod "/dev/fuse" :c 10 229)
            (start-daemon! lazyfs))
        (throw+ e))))
  lazyfs)

(defn umount!
  "Stops the given lazyfs map. You probably want to call this as a part of
  database teardown."
  [{:keys [dir]}]
  (try+
    (info "Unmounting lazyfs" dir)
    (c/exec :fusermount :-uz dir)
    (catch [:exit 1] _
      ; No such directory
      nil)
    (catch [:exit 127] _
      ; Command not found
      nil)))

(defn fifo!
  "Sends a string to the fifo channel for the given lazyfs map."
  [{:keys [fifo]} cmd]
  (timeout 1000 (throw+ {:type ::fifo-timeout
                         :cmd  cmd
                         :fifo fifo})
           ;(info :echo cmd :> fifo)
           (c/exec :echo cmd :> fifo)))

(defrecord DB [lazyfs]
  db/DB
  (setup! [this test node]
    (install!)
    (mount! lazyfs))

  (teardown! [this test node]
    (umount! lazyfs))

  db/LogFiles
  (log-files [this test node]
    {"lazyfs.log" (:log-file lazyfs)}))

(defn db
  "Takes a lazyfs map and constructs a DB whose setup installs lazyfs and
  mounts the given lazyfs dir."
  [lazyfs]
  (DB. lazyfs))

(defn nemesis
  "A nemesis which inject faults into the given lazyfs map by writing to its
  fifo. Types of faults (:f) supported:

  :lose-unfsynced-writes

      Forgets any writes which were not fsynced. The
      :value should be a list of nodes you'd like to lose un-fsynced writes on."
  [lazyfs]
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (case (:f op)
        :lose-unfsynced-writes
        (let [v (c/on-nodes test (:value op)
                            (fn [_ _]
                              (do (fifo! lazyfs "lazyfs::clear-cache")
                                  :done)))]
            (assoc op :value v))))

    (teardown! [this test])

    nemesis/Reflection
    (fs [this]
      #{:lose-unfsynced-writes})))
