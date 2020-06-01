(ns jepsen.stolon.db
  "Database setup and automation."
  (:require [cheshire.core :as json]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
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

(defn stolonctl!
  "Calls stolonctl with args."
  [& args]
  (c/sudo user
    (c/cd dir
          (apply c/exec "bin/stolonctl" args))))

(defn store-endpoints
  "Computes the etcd address for stolon commands"
  []
  (str "http://" (cn/local-ip) ":2379"))

(defn init-cluster!
  "Sets up Stolon's cluster data. Maybe not necessary if we tell sentinels about init spec?"
  [test node]
  (c/sudo user
    (stolonctl! :--cluster-name "jepsen-cluster"
              "--store-backend=etcdv3"
              :--store-endpoints (store-endpoints)
              :init
              :--initial-cluster-spec)))

(defn initial-cluster-spec
  "The data structure we use for our stolon initial cluster specification. See
  https://github.com/sorintlab/stolon/blob/master/doc/cluster_spec.md for
  options."
  [test]
  {:synchronousReplication true
   :initMode               :new})

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

(defn start-keeper!
  "Starts a Stolon keeper process."
  [test node]
  (let [uid (str "pg" (.indexOf (:nodes test) node))]
    (c/sudo user
      (cu/start-daemon!
        {:chdir   dir
         :logfile keeper-logfile
         :pidfile keeper-pidfile}
        (str dir "/bin/" keeper-bin)
        :--cluster-name       cluster-name
        :--store-backend      "etcdv3"
        :--store-endpoints    (store-endpoints)
        :--uid                uid
        :--data-dir           (str data-dir "/" uid)
        :--pg-su-password     "pw"
        :--pg-repl-username   "repluser"
        :--pg-repl-password   "pw"
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

(defrecord Stolon [etcd etcd-test]
  db/DB
  (setup! [db test node]
    (db/setup! etcd (etcd-test test) node)
    (install-pg!      test node)
    (install-stolon!  test node)
    ; (init-cluster! test node)
    (start-sentinel!  test node)
    (start-keeper!    test node)
    (start-proxy!     test node)
    (sc/close! (sc/await-open node)))

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
            (db/log-files etcd (etcd-test test) node))))

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
              (c/exec :rm :-rf (c/lit "/var/lib/postgresql/12/main/*")))
        (c/sudo user
                (c/exec :truncate :-s 0 just-postgres-log-file))
        (db/teardown! tcpdump test node))

      db/LogFiles
      (log-files [_ test node]
        (concat (db/log-files tcpdump test node)
                [just-postgres-log-file])))))

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
