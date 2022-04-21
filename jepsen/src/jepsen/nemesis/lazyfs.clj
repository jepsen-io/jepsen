(ns jepsen.nemesis.lazyfs
  "The lazyfs nemesis allows the injection of filesystem-level faults:
  specifically, losing data which was written to disk but not fsynced."
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [util :as util]
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
  "1f6c3cd021612866a047195c53201bc7b22b4326")

(def dir
  "Where do we install lazyfs to on the remote node?"
  "/opt/jepsen/lazyfs")

(def bin
  "The lazyfs binary"
  (str dir "/lazyfs/build/lazyfs"))

(def fifo
  "The fifo we use to control lazyfs."
  "/opt/jepsen/lazyfs.fifo")

(def config
  "The lazyfs config file"
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
          (c/exec :git :checkout commit)
          (c/exec :git :clean :-fx)
          ; Install libpcache
          (c/cd "libs/libpcache"
                (c/exec "./build.sh"))
          ; Install lazyfs
          (c/cd "lazyfs"
            (c/exec "./build.sh")))))

(defn mount!
  "Starts lazyfs as a daemon at the given directory. You likely want to call
  this before beginning database setup."
  [dir]
  (info "Mounting lazyfs" dir)
  (let [real-dir    (str dir real-extension)
        config-path (cu/tmp-file!)]
    ; Write config file
    (cu/write-file! config config-path)
    ; Make directories
    (c/exec :mkdir :-p dir)
    (c/exec :mkdir :-p real-dir)
    ; And go!
    (try+
      (c/exec bin dir :--config-path config-path :-o :allow_other :-o "modules=subdir" :-o (str "subdir=" real-dir))
      (catch [:exit -1] e
        (if (re-find #"fuse: device not found" (:err e))
          (do (c/exec :mknod "/dev/fuse" :c 10 229)
              (c/exec bin dir :--config-path config-path :-o :allow_other :-o "modules=subdir" :-o (str "subdir=" real-dir)))
          (throw+ e))))))

(defn umount!
  "Stops lazyfs at the given directory. You probably want to call this as a
  part of database teardown."
  [dir]
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
  "Sends a string to the fifo channel."
  [cmd]
  (c/exec :echo cmd :> fifo))

(defn db
  "A database which responds to setup! and teardown! by making the specified
  directory a lazyfs."
  [dir]
  (reify db/DB
    (setup! [this test node]
      (install!)
      (mount! dir))

    (teardown! [this test node]
      (umount! dir))))

(defn nemesis
  "A nemesis which inject faults into the currently mounted lazyfs by writing
  to the lazyfs fifo. Types of faults (:f) supported:

  :lose-unfsynced-writes

      Forgets any writes which were not fsynced. The
      :value should be a list of nodes you'd like to lose un-fsynced writes on."
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      this)

    (invoke! [this test op]
      (case (:f op)
        :lose-unfsynced-writes
        (let [v (c/on-nodes test (:value op)
                            (fn [_ _]
                              (do (fifo! "lazyfs::clear-cache")
                                  :done)))]
            (assoc op :value v))))

    (teardown! [this test])))
