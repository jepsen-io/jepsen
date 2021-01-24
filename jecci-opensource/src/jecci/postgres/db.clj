(ns jecci.postgres.db
  "Tests for postgres"
  (:require [clojure.tools.logging :refer :all]
            [clojure.stacktrace :as cst]
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
            [clojure.java.jdbc :as j]
            [jecci.utils.handy :as juh]
            [jecci.utils.db :as jud])
  (:use [slingshot.slingshot :only [throw+ try+]]))

(def home "/home/jecci")
(def pg-dir "/home/jecci/postgresql")
(def pg-install-dir "/home/jecci/pginstall")
(def pg-bin-dir (str pg-install-dir "/bin"))
(def pg-postgres-file (str pg-bin-dir "/postgres"))
(def pg-initdb-file (str pg-bin-dir "/initdb"))
(def pg-pg_ctl-file (str pg-bin-dir "/pg_ctl"))
(def pg-psql-file (str pg-bin-dir "/psql"))
(def pg-pg_basebackup-file (str pg-bin-dir "/pg_basebackup"))
(def pg-pg_isready-file (str pg-bin-dir "/pg_isready"))

(def pg-data "/home/jecci/pgdata")
(def pg-config-file (str pg-data "/postgresql.conf"))
(def pg-hba-file (str pg-data "/pg_hba.conf"))
(def pg-log-file (str pg-data "/pg.log"))
(def pg_ctl-stdout (str pg-data "/pg_ctl.stdout"))

(def repeatable-read "shared_buffers=4GB\nlisten_addresses = '*'\nwal_keep_size=16GB\n
default_transaction_isolation = 'repeatable read'")
(def serializable "shared_buffers=4GB\nlisten_addresses = '*'\nwal_keep_size=16GB\n
default_transaction_isolation = 'serializable'")

; leader of pg cluster, should contain only one node
(def leaders ["n1"])
(def backups ["n2" "n3" "n4" "n5"])

(def hba-appends
  (str/join "\n"
            (for [db ["replication" "all"]
                  :let [line ((fn [db] (str "host    "
                                            db
                                            "         all         "
                                            "0.0.0.0/0"
                                            "    trust"))
                              db)]]
              line)))

(defn pg-initdb
  "Inits postgres"
  []
  (c/exec "bash" :-c (str pg-initdb-file " -n -D " pg-data " -E utf-8 ")))

(defn get-leader-node
  "Get leader, should return one node"
  []
  (first leaders))

(defn get-followers-node
  "Get followers, should return a list"
  []
  backups)

(defn pg-start!
  "Starts the pg daemons on the cluster"
  [_ _]
  (try+
   (c/su (c/exec "pkill" :-9 :-e :-c :-U "jecci" :-f "postgres" :|| :echo "no process to kill"))
   (juh/exec->info (c/exec pg-pg_ctl-file "-D" pg-data "-l" pg-log-file "start"))
   (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
     nil)))

(defn pg-stop!
  "Stops postgres"
  [test node]
  (try+
   (juh/exec->info (c/exec pg-pg_ctl-file "-D" pg-data "stop"))
   (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
     nil)))

(defn start-wait-pg!
  "Starts pg, waiting for pg_isready."
  [test node]
  (jud/restart-loop :postmaster (pg-start! test node)
                    (try+
                     (juh/exec->info (c/exec pg-pg_isready-file))
                     :ready
                     (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
                       (when (str/includes? (str err) "no response") :crashed)
                       (when (str/includes? (str err) "rejecting connections") :starting)))))



(defn tarball-url
  "Return the URL for a tarball"
  [test]
  (or (:tarball-url test)
      (str "https://github.com/postgres/postgres/archive/REL_13_0.tar.gz")))


(defn isleader?
  [node]
  (contains? (into #{} leaders) node))

(defn setup-faketime!
  "Configures the faketime wrapper for this node, so that the given binary runs
  at the given rate."
  [bin rate]
  (info "Configuring" bin "to run at" (str rate "x realtime"))
  (faketime/wrap! pg-postgres-file 0 rate))

(defn install!
  "Downloads archive and extracts it to our local pg-dir, if it doesn't exist
  already. If test contains a :force-reinstall key, we always install a fresh
  copy.

  Calls `sync`"
  [test node]
  (c/su
   (c/exec "pkill" :-9 :-e :-c :-U "jecci" :-f "postgres" :|| :echo "no process to kill")
   (c/exec :rm :-rf pg-data)
   (c/exec :rm :-rf pg-dir))
  (when (or (:force-reinstall test)
            (not (cu/exists? pg-install-dir)))
    (info node "installing postgresql")
    (cu/install-archive! (tarball-url test) pg-dir {:user? "jecci", :pw? "123456"})
    
    (c/exec "mkdir" "-p" pg-data)
    (c/cd pg-dir
          (c/exec (str pg-dir "/configure") (str "--prefix=" pg-install-dir) "--enable-depend" "--enable-cassert"
                  "--enable-debug"  "--without-readline" "--without-zlib")
          (juh/exec->info  (c/exec "make" "-j8" "-s"))
          (juh/exec->info  (c/exec "make" "install" "-j8" "-s")))))

(defn db
  "postgres"
  []
  (reify db/DB
    (setup! [db test node]
      (info "setting up postgres")
      (c/su (juh/exec->info (c/exec "apt" "install" "-qy" "bison" "flex")))

      (c/cd home
            (install! test node)

            (when (isleader? node)
              (info node "initing leader postgres")

              (juh/exec->info (pg-initdb))
              (c/exec "echo" :-e (if (= (:workload test) :append) serializable repeatable-read)
                      :>> pg-config-file)
              (c/exec "echo" :-e hba-appends :>> pg-hba-file)

              (c/sudo "jecci"
                      (if-let [ratio (:faketime test)]
                   ; Add faketime wrappers
                        (setup-faketime! pg-postgres-file (faketime/rand-factor ratio))
                        (c/cd pg-dir
                   ; Destroy faketime wrappers, if applicable.
                              (faketime/unwrap! pg-postgres-file))))

              ; To use faketime wrapper, we must start pg by running postgres executable
              ; directly, because if we use wrapper, the parent pid of postgres will not be the
              ; one we run, which is not allowed by pg_ctl
              ; 
              ; Will change to use this kind of start for pg in the future. Will also change
              ; to use start-daemon! as well.
                   ;(juh/exec->info (c/exec "bash" :-c (str "nohup \"" pg-postgres-file
                         ;             "\" " "-D " pg-data " < \"" "/dev/null"
                           ;           "\" >> \"" pg-log-file "\" 2>&1 &")))
              (juh/exec->info (c/exec pg-pg_ctl-file :-D pg-data :-l pg-log-file "start"))))

      (info node "finish installing" (str (when (isleader? node) " and init for leaders")))
      (jepsen/synchronize test 180)

      (when (contains? (into #{} backups) node)
        (info node "starting backup")
        (c/cd home
              (c/su (c/exec :rm :-rf pg-data))
              (c/exec "mkdir" :-p pg-data)
              (juh/change-permit pg-data)
              (locking test (juh/exec->info (c/exec pg-pg_basebackup-file
                                                    :-R :-D pg-data :-h (first leaders) :-p "5432")))
              (c/exec "echo" :-e hba-appends :>> pg-hba-file)
              (c/exec "echo" :-e
                      "shared_buffers=128MB\nlisten_addresses = '*'" :>> pg-config-file)

              (c/sudo "jecci"
                      (if-let [ratio (:faketime test)]
              ; Add faketime wrappers
                        (setup-faketime! pg-postgres-file (faketime/rand-factor ratio))
                        (c/cd pg-dir
                ; Destroy faketime wrappers, if applicable.
                              (faketime/unwrap! pg-postgres-file))))
              ; same as leader, will change the way we start pg in the future
              (juh/exec->info (c/exec pg-pg_ctl-file :-D pg-data :-l pg-log-file "start"))))

      (info node "finish starting up")
      (jepsen/synchronize test 180))

    (teardown! [db test node]
      (try+
       (pg-stop! test node)
       (c/exec :rm :-rf (str pg-data "/*"))
       (catch [:type :jepsen.control/nonzero-exit] {:keys [err]}
          ; No such dir
         (warn (str err))))
      (info node "finish tearing down"))

    ; Following files will be downloaded from nodes
    ; When test fail or finish
    db/LogFiles
    (log-files [_ test node]
      [pg-log-file])))
