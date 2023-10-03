(ns jepsen.lazyfs
  "Lazyfs allows the injection of filesystem-level faults: specifically, losing
  data which was written to disk but not fsynced. This namespace lets you mount
  a specific directory as a lazyfs filesystem, and offers a DB which
  mounts/unmounts it, and downloads the lazyfs log file--this can be composed
  into your own database. You can then call lose-unfsynced-writes! as a part of
  your database's db/kill! implementation, likely after killing your DB process
  itself."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [util :as util :refer [await-fn meh timeout]]
                    [nemesis :as nemesis]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.io File)))

(def repo-url
  "Where can we clone lazyfs from?"
  "https://github.com/dsrhaslab/lazyfs.git")

(def commit
  "What version should we check out and build?"
  "0.2.0")

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
  [{:keys [log-file fifo fifo-completed cache-size]}]
  (str "[faults]
fifo_path=\"" fifo "\"
# Not sure how to use this yet.
#fifo_path_completed=\"" fifo-completed "\"

[cache]
apply_eviction=false

[cache.simple]
custom_size=\"" (or cache-size "0.5GB") "\"
blocks_per_page=1

[filesystem]
logfile=\"" log-file "\"
log_all_operations=false
"))

(def real-extension
  "When we mount a lazyfs directory, it's backed by a real directory on the
  underlying filesystem: e.g. 'foo' is backed by 'foo.real'. We name this
  directory using this extension."
  ".real")

(defn install!
  "Installs lazyfs on the currently-bound remote node."
  []
  (info "Installing lazyfs" commit)
  (c/su
    ; Dependencies
    (debian/install [:g++ :cmake :libfuse3-dev :libfuse3-3 :fuse3 :git])
    ; LXC containers like to delete /dev/fuse on reboot for some reason
    (when-not (cu/exists? fuse-dev)
      ; We're going to create a fuse device that's writable by everyone. This
      ; would be unsafe on normal systems, but Jepsen DB nodes are intended to
      ; be a disposable playground.
      ;
      ; This is an awful hack, but figuring out how to cleanly get permissions
      ; to the right users while also working with systems like debian 3 which
      ; don't HAVE a fuse group any more is... well I'm not sure I can do it
      ; safely/reliably.
      (c/exec :mknod fuse-dev "c" 10 229)
      (c/exec :chmod "a+rw" fuse-dev))
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
  "Takes a directory as a string, or a map of options, or a full lazyfs map,
  which is passed through unaltered. Constructs a lazyfs map of all the files
  we need to run a lazyfs for a directory. Map options are:

    :dir      The directory to mount
    :user     Which user should run lazyfs? Default \"root\".
    :chown    Who to set as the owner of the directory. Defaults to
              \"<user>:<user>\"
    :cache-size  The size of the lazyfs page cache. Should be a string like
                 \"0.5GB\"
  "
  [x]
  (let [opts (cond (string? x)
                   {:dir x}

                   (map? x)
                   x

                   true
                   (throw (IllegalArgumentException.
                            (str "Expected a string directory or a lazyfs map, but got "
                                 (pr-str x)))))
        dir         (:dir         opts)
        lazyfs-dir  (:lazyfs-dir  opts (str dir ".lazyfs"))
        data-dir    (:data-dir    opts (str lazyfs-dir "/data"))
        fifo        (:fifo        opts (str lazyfs-dir "/fifo"))
        fifo-completed (:fifo-completed opts (str lazyfs-dir "/fifo-completed"))
        config-file (:config-file opts (str lazyfs-dir "/config"))
        log-file    (:log-file    opts (str lazyfs-dir "/log"))
        user        (:user        opts "root")
        chown       (:chown       opts (str user ":" user))]
    {:dir         dir
     :lazyfs-dir  lazyfs-dir
     :data-dir    data-dir
     :fifo        fifo
     :fifo-completed fifo-completed
     :config-file config-file
     :log-file    log-file
     :user        user
     :chown       chown}))

(defn start-daemon!
  "Starts the lazyfs daemon once preparation is complete. We daemonize
  ourselves so that we can get logs--also it looks like the built-in daemon
  might not work right now."
  [opts]
  ; This explodes if you run it anywhere other than the lazyfs dir
  (c/cd (str dir "/lazyfs")
        (c/exec "scripts/mount-lazyfs.sh"
                :-c (:config-file opts)
                :-m (:dir opts)
                :-r (:data-dir opts))))

(defn mount!
  "Takes a lazyfs map, creates directories and config files, and starts the
  lazyfs daemon. You likely want to call this before beginning database setup.
  Returns the lazyfs map."
  [{:keys [dir data-dir lazyfs-dir chown user config-file log-file] :as lazyfs}]
  (c/su
    (info "Mounting lazyfs" dir)
    ; Make directories
    (c/exec :mkdir :-p dir)
    (c/exec :mkdir :-p data-dir)
    (c/exec :chown chown dir)
    (c/exec :chown :-R chown lazyfs-dir))
  (c/sudo user
    ; Create log file
    (c/exec :touch log-file)
    ; Write config file
    (cu/write-file! (config lazyfs) config-file)
    ; And go!
    (start-daemon! lazyfs))
  ; Await mount
  (c/su
    (await-fn (fn check-mounted []
                (or (re-find #"lazyfs" (c/exec :findmnt dir))
                    (throw+ {:type ::not-mounted
                             :dir  dir
                             :node c/*host*})))
              {:retry-interval 500
               :log-interval   5000
               :log-message "Waiting for lazyfs to mount"}))
  lazyfs)

(declare lose-unfsynced-writes!)

(defn umount!
  "Stops the given lazyfs map and destroys the lazyfs directory. You probably
  want to call this as a part of database teardown."
  [{:keys [lazyfs-dir dir] :as lazyfs}]
  (c/su
    (try+
      ; I think it flushes on umount which can take *forever*, so we drop the
      ; page cache here to try and speed that up.
      (meh (lose-unfsynced-writes! lazyfs))
      (info "Unmounting lazyfs" dir)
      (c/exec :fusermount :-uz dir)
      (info "Unmounted lazyfs" dir)
      (catch [:exit 1] _
        ; No such directory
        nil)
      (catch [:exit 127] _
        ; Command not found
        nil))
    (c/exec :rm :-rf lazyfs-dir)))

(defn fifo!
  "Sends a string to the fifo channel for the given lazyfs map."
  [{:keys [user fifo fifo-completed]} cmd]
  (timeout 1000 (throw+ {:type ::fifo-timeout
                         :cmd  cmd
                         :node c/*host*
                         :fifo fifo})
           ;(info :echo cmd :> fifo)
           (c/sudo user (c/exec :echo cmd :> fifo))))

(defrecord DB [lazyfs]
  db/DB
  (setup! [this test node]
    (install!)
    (mount! lazyfs))

  (teardown! [this test node]
    (umount! lazyfs))

  db/LogFiles
  (log-files [this test node]
    {(:log-file lazyfs) "lazyfs.log"}))

(defn db
  "Takes a directory or a lazyfs map and constructs a DB whose setup installs
  lazyfs and mounts the given lazyfs dir."
  [dir-or-lazyfs]
  (DB. (lazyfs dir-or-lazyfs)))

(defn lose-unfsynced-writes!
  "Takes a lazyfs map or a lazyfs DB. Asks the local node to lose any writes to
  the given lazyfs map which have not been fsynced yet."
  [db-or-lazyfs-map]
  (if (instance? DB db-or-lazyfs-map)
    (recur (:lazyfs db-or-lazyfs-map))
    (do (info "Losing un-fsynced writes to" (:dir db-or-lazyfs-map))
        (fifo! db-or-lazyfs-map "lazyfs::clear-cache")
        :done)))

(defn checkpoint!
  "Forces the given lazyfs map or DB to flush writes to disk."
  [db-or-lazyfs-map]
  (if (instance? DB db-or-lazyfs-map)
    (recur (:lazyfs db-or-lazyfs-map))
    (do (info "Checkpointing all writes to" (:dir db-or-lazyfs-map))
        (fifo! db-or-lazyfs-map "lazyfs::cache-checkpoint")
        :done)))

(defn nemesis
  "A nemesis which inject faults into the given lazyfs map by writing to its
  fifo. Types of faults (:f) supported:

  :lose-unfsynced-writes

      Forgets any writes which were not fsynced. The
      :value should be a list of nodes you'd like to lose un-fsynced writes on.

  You don't necessarily need to use this--I haven't figured out how to
  integrate it well into jepsen.nemesis combined. Once we start getting other
  classes of faults it will probably make sense for this nemesis to get more
  use and expand."
  [lazyfs]
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (case (:f op)
        :lose-unfsynced-writes
        (let [v (c/on-nodes test (:value op)
                            (fn [_ _] (lose-unfsynced-writes! lazyfs)))]
            (assoc op :value v))))

    (teardown! [this test])

    nemesis/Reflection
    (fs [this]
      #{:lose-unfsynced-writes})))
