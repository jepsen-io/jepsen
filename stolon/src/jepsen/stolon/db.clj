(ns jepsen.stolon.db
  "Database setup and automation."
  (:require [cheshire.core :as json]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [disorderly
                                  real-pmap]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh random-nonempty-subset]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.etcd.db :as etcd]
            [jepsen.os.debian :as debian]
            [jepsen.stolon [client :as sc]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def dir "/opt/stolon")
(def data-dir (str dir "/data"))
(def user
  "The OS user which will run Stolon, postgres, etc."
  "postgres")

(def sentinel-logfile (str dir "/sentinel.log"))
(def sentinel-pidfile (str dir "/sentinel.pid"))
(def sentinel-bin "stolon-sentinel")

(def keeper-logfile (str dir "/keeper.log"))
(def keeper-pidfile (str dir "/keeper.pid"))
(def keeper-bin "stolon-keeper")

(def proxy-logfile (str dir "/proxy.log"))
(def proxy-pidfile (str dir "/proxy.pid"))
(def proxy-bin "stolon-proxy")

(def cluster-name "jepsen-cluster")

(def just-postgres-log-file
  "/var/log/postgresql/postgresql-12-main.log")

(defn install-pg!
  "Installs postgresql"
  [test node]
  (c/su
    ; Install apt key
    (c/exec :wget :--quiet :-O :- "https://www.postgresql.org/media/keys/ACCC4CF8.asc" c/| :apt-key :add :-)
    ; Add repo
    (debian/install [:lsb-release])
    (let [release (c/exec :lsb_release :-cs)]
      (debian/add-repo! "postgresql"
                        (str "deb http://apt.postgresql.org/pub/repos/apt/ "
                             release "-pgdg main")))
    ; Install
    (debian/install [:postgresql-12 :postgresql-client-12])
    ; Deactivate default install
    (c/exec :service :postgresql :stop)
    (c/exec "update-rc.d" :postgresql :disable)))

(defn install-stolon!
  "Downloads and installs Stolon."
  [test node]
  (c/su
    (let [version (:version test)]
      (cu/install-archive! (str "https://github.com/sorintlab/stolon/releases/download/v"
                                version "/stolon-v" version "-linux-amd64.tar.gz")
                           dir))
    (c/exec :chown :-R (str user ":" user) dir)))

(defn store-endpoints
  "Computes the etcd address for stolon commands"
  []
  (str "http://" (cn/local-ip) ":2379"))

(defn stolonctl!
  "Calls stolonctl with args. Provides cluster name and store connection args
  automatically."
  [& args]
  (c/sudo user
    (c/cd dir
          (apply c/exec "bin/stolonctl"
                 :--cluster-name "jepsen-cluster"
                 :--store-backend "etcdv3"
                 :--store-endpoints (store-endpoints)
                 args))))

(defn initial-cluster-spec
  "The data structure we use for our stolon initial cluster specification. See
  https://github.com/sorintlab/stolon/blob/master/doc/cluster_spec.md for
  options."
  [test]
  {:synchronousReplication true
   :initMode               :new
   :sleepInterval          "1s"
   :requestTimeout         "2s"
   :failInterval           "4s"
   :proxyCheckInterval     "1s"
   :proxyTimeout           "3s"
   ; TODO: this is set to 48 hours, and it feels DANGEROUS, let's make it small
   ; later.
   :deadKeeperRemovalInterval "48h"
   :maxStandbysPerSender    (dec (count (:nodes test)))
   ; Default is 1, but I'm pretty sure that allows bad things to happen.
   :minSynchronousStandbys  1
   :maxSynchronousStandbys  1
   })

(defn start-sentinel!
  "Starts a Stolon sentinel process."
  [test node]
  (c/cd dir
        (c/sudo user
                (let [init-spec "init-spec.json"]
                (c/exec :echo (json/generate-string
                               (initial-cluster-spec test))
                        :> init-spec)
                (cu/start-daemon!
                  {:chdir   dir
                   :logfile sentinel-logfile
                   :pidfile sentinel-pidfile}
                  (str dir "/bin/" sentinel-bin)
                  :--cluster-name     cluster-name
                  :--store-backend    "etcdv3"
                  :--store-endpoints  (store-endpoints)
                  :--initial-cluster-spec init-spec)))))

(defn node->pg-id
  "Turns a node into a postgres ID, e.g. pg1"
  [test node]
  (str "pg" (inc (.indexOf (:nodes test) node))))

(defn pg-id->node
  "Turns a postgres ID into a node."
  [test pg-id]
  (nth (:nodes test)
       (Long/parseLong ((re-find #"pg(.+)" pg-id) 1))))

(defn start-keeper!
  "Starts a Stolon keeper process."
  [test node]
  (let [pg-id (node->pg-id test node)]
    (c/sudo user
      (cu/start-daemon!
        {:chdir   dir
         :logfile keeper-logfile
         :pidfile keeper-pidfile}
        (str dir "/bin/" keeper-bin)
        :--cluster-name       cluster-name
        :--store-backend      "etcdv3"
        :--store-endpoints    (store-endpoints)
        :--uid                pg-id
        :--data-dir           (str data-dir "/" pg-id)
        :--pg-su-password     (:postgres-password test)
        :--pg-repl-username   "repluser"
        :--pg-repl-password   (:postgres-password test)
        :--pg-listen-address  (cn/local-ip)
        :--pg-port            5433
        :--pg-bin-path        "/usr/lib/postgresql/12/bin"))))

(defn start-proxy!
  "Starts the Stolon proxy process."
  [test node]
  (c/sudo user
          (cu/start-daemon!
            {:chdir dir
             :logfile proxy-logfile
             :pidfile proxy-pidfile}
            (str dir "/bin/" proxy-bin)
            :--cluster-name     cluster-name
            :--store-backend    "etcdv3"
            :--store-endpoints  (store-endpoints)
            :--listen-address   "0.0.0.0")))

(defn stop-sentinel!
  "Kills the sentinel process."
  [test node]
  (c/sudo user
    (cu/stop-daemon! sentinel-bin sentinel-pidfile)))

(defn stop-keeper!
  "Kills the keeper process."
  [test node]
  (c/sudo user
    (cu/stop-daemon! keeper-bin keeper-pidfile)))

(defn stop-proxy!
  "Kills the proxy process."
  [test node]
  (c/sudo user
          (cu/stop-daemon! proxy-bin proxy-pidfile)))

(defn status
  "Returns the status of the stolon cluster, as a string."
  []
  (c/sudo user (stolonctl! :status)))

(defn primary-keeper
  "Returns the keeper the current node thinks is primary."
  [test]
  (let [status (status)]
    (if-let [match (re-find #"Master Keeper:\s+(.+)\n" status)]
      (pg-id->node test (nth match 1))
      (info :couldn't-parse status))))

(defrecord Stolon [etcd etcd-test]
  db/DB
  (setup! [db test node]
    (db/setup! etcd (etcd-test test) node)
    (install-pg!      test node)
    (install-stolon!  test node)
    (start-sentinel!  test node)
    (start-keeper!    test node)
    (start-proxy!     test node)
    (sc/close! (sc/await-open test node)))

  (teardown! [db test node]
    (stop-proxy!    test node)
    (stop-keeper!   test node)
    (stop-sentinel! test node)
    (c/su (c/exec :rm :-rf dir))
    (db/teardown! etcd (etcd-test test) node))

  db/LogFiles
  (log-files [db test node]
    (concat [sentinel-logfile
             keeper-logfile
             proxy-logfile]
            (db/log-files etcd (etcd-test test) node)))

  db/Primary
  (setup-primary! [db test node])

  (primaries [db test]
    (->> (c/on-nodes test (fn [test node]
                            (try+
                              (primary-keeper test)
                              (catch [:exit 1] e
                                ; Ah well
                                nil))))
         vals
         (remove nil?)
         distinct)))


(defn db
  "Sets up stolon"
  [opts]
  (Stolon.
    (etcd/db)
    ; A function which transforms our test into a test for an etcd DB.
    (let [initialized? (atom false)
          members      (atom (into (sorted-set) (:nodes opts)))]
      (fn [test]
        (assoc test
               :version      (:etcd-version test)
               :initialized? initialized?
               :members      members)))))

(defn just-postgres
  "A database which just runs a regular old Postgres instance, to help rule out
  bugs in Stolon specifically."
  [opts]
  (let [tcpdump (db/tcpdump {:ports [5432]
                             ; Haaack, hardcoded for my particular cluster
                             ; control node
                             :filter "host 192.168.122.1"})]
    (reify db/DB
      (setup! [_ test node]
        (db/setup! tcpdump test node)
        (install-pg! test node)
        (c/su (c/exec :echo (slurp (io/resource "pg_hba.conf"))
                      :> "/etc/postgresql/12/main/pg_hba.conf")
              (c/exec :echo (slurp (io/resource "postgresql.conf"))
                      :> "/etc/postgresql/12/main/postgresql.conf"))

        (c/sudo user
                (c/exec "/usr/lib/postgresql/12/bin/initdb"
                        :-D "/var/lib/postgresql/12/main"))

        (c/su (c/exec :service :postgresql :start)))

      (teardown! [_ test node]
        (c/su (c/exec :service :postgresql :stop)
              ; This might not actually work, so we have to kill the processes
              ; too
              (cu/grepkill! "postgres")
              (c/exec :rm :-rf (c/lit "/var/lib/postgresql/12/main/*")))
        (c/sudo user
                (c/exec :truncate :-s 0 just-postgres-log-file))
        (db/teardown! tcpdump test node))

      db/LogFiles
      (log-files [_ test node]
        (concat (db/log-files tcpdump test node)
                [just-postgres-log-file]))

      db/Primary
      (setup-primary! [db test node])
      (primaries [db test]
        ; Everyone's a winner! Really, there should only be one node here,
        ; so... it's kinda trivial.
        (:nodes test))

      db/Process
      (start! [db test node]
        (c/su (c/exec :service :postgresql :restart)))

      (kill! [db test node]
        (doseq [pattern (shuffle
                          ["postgres -D" ; Main process
                           "main: checkpointer"
                           "main: background writer"
                           "main: walwriter"
                           "main: autovacuum launcher"])]
          (Thread/sleep (rand-int 100))
          (info "Killing" pattern "-" (cu/grepkill! pattern)))))))
