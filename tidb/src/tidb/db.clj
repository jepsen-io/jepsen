(ns tidb.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
              [core :as jepsen]
              [control :as c]
              [db :as db]
            ]
            [jepsen.control.util :as cu]
            [tidb.util :refer :all]
  )
)

(def tidb-url "http://download.pingcap.org/tidb-latest-linux-amd64.tar.gz")
(def tidb-dir "/opt/tidb")
(def tipd "./bin/pd-server")
(def tikv "./bin/tikv-server")
(def tidb "./bin/tidb-server")
(def tipdbin "pd-server")
(def tikvbin "tikv-server")
(def tidbbin "tidb-server")
(def pdlogfile (str tidb-dir "/jepsen-pd.log"))
(def pdpidfile (str tidb-dir "/jepsen-pd.pid"))
(def kvlogfile (str tidb-dir "/jepsen-kv.log"))
(def kvpidfile (str tidb-dir "/jepsen-kv.pid"))
(def dblogfile (str tidb-dir "/jepsen-db.log"))
(def dbpidfile (str tidb-dir "/jepsen-db.pid"))
(def pdconfigfile (str tidb-dir "/pd.conf"))
(def log-file "test.log")

(def client-port 2379)
(def peer-port   2380)

(def tidb-map
  {:n1 {:pd "pd1" :kv "tikv1"}
   :n2 {:pd "pd2" :kv "tikv2"}
   :n3 {:pd "pd3" :kv "tikv3"}
   :n4 {:pd "pd4" :kv "tikv4"}
   :n5 {:pd "pd5" :kv "tikv5"}
  }
)

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port)
)

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node client-port)
)

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node peer-port)
)

(defn initial-cluster
  "Constructs an initial cluster string for a test, like \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (get-in tidb-map [node :pd]) "=" (peer-url node))))
       (str/join ",")
  )
)

(defn pd-endpoints
  "Constructs an initial pd cluster string for a test, like \"foo:2379,bar:2379,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (name node) ":" client-port)))
       (str/join ",")
  )
)

(defn db
  "TiDB"
  [opts]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing TiDB")
        (cu/install-tarball! node tidb-url tidb-dir)

        ; ./bin/pd-server --name=pd1
        ;                 --data-dir=pd1
        ;                 --client-urls="http://0.0.0.0:2379"
        ;                 --peer-urls="http://0.0.0.0:2380"
        ;                 --advertise-client-urls="http://n1:2379"
        ;                 --advertise-peer-urls="http://n1:2380"
        ;                 --initial-cluster="pd1=http://n1:2380, \
        ;                                    pd2=http://n2:2380, \
        ;                                    pd3=http://n3:2380" \
        ;                                    pd4=http://n4:2380" \
        ;                                    pd5=http://n5:2380" \
        ;                 --log-file=pd.log
        (info node "installing mysql-client")
        (c/exec :apt-get :install :-y "mysql-client")

        (c/exec :echo "[replication]\nmax-replicas=5" :> pdconfigfile)
        (cu/start-daemon!
          {:logfile pdlogfile
           :pidfile pdpidfile
           :chdir   tidb-dir
          }
          tipd
          :--name                  (get-in tidb-map [node :pd])
          :--data-dir              (get-in tidb-map [node :pd])
          :--client-urls           (str "http://0.0.0.0:" client-port)
          :--peer-urls             (str "http://0.0.0.0:" peer-port)
          :--advertise-client-urls (client-url node)
          :--advertise-peer-urls   (peer-url node)
          :--initial-cluster       (initial-cluster test)
          :--log-file              (str "pd.log")
          :--config                pdconfigfile
        )

        (Thread/sleep 10000)
        (jepsen/synchronize test)

        ; ./bin/tikv-server --pd="n1:2379,n2:2379,n3:2379,n4:2379,n5:2379"
        ;                   --addr="0.0.0.0:20160"
        ;                   --advertise-addr="n1:20160"
        ;                   --data-dir=tikv1
        ;                   --log-file=tikv.log
        (cu/start-daemon!
          {:logfile kvlogfile
           :pidfile kvpidfile
           :chdir   tidb-dir
          }
          tikv
          :--pd             (pd-endpoints test)
          :--addr           (str "0.0.0.0:20160")
          :--advertise-addr (str (name node) ":" "20160")
          :--data-dir       (get-in tidb-map [node :kv])
          :--log-file       (str "tikv.log")
        )

        (Thread/sleep 30000)
        (jepsen/synchronize test)

        ; ./bin/tidb-server --store=tikv
        ;                   --path="n1:2379,n2:2379,n3:2379,n4:2379,n5:2379"
        ;                   --log-file=tidb.log
        (cu/start-daemon!
          {:logfile dblogfile
           :pidfile dbpidfile
           :chdir   tidb-dir
          }
          tidb
          :--store     (str "tikv")
          :--path      (pd-endpoints test)
          :--log-file  (str "tidb.log")
        )

        (Thread/sleep 5000)
        (jepsen/synchronize test)

        (sql! "create database if not exists jepsen;")
        (sql! (str "GRANT ALL PRIVILEGES ON jepsen.* TO \'jepsen\'@\'%\' IDENTIFIED BY \'jepsen\';"))
      )
    )
    (teardown! [_ test node]
      (info node "tearing down TiDB")
      (cu/stop-daemon! tidbbin dbpidfile)
      (cu/stop-daemon! tikvbin kvpidfile)
      (cu/stop-daemon! tipdbin pdpidfile)
      (cu/grepkill! "mysqld")
      (cu/grepkill! "mysqld_safe")
      ;(c/su (c/exec :rm :-rf tidb-dir))
    )

    db/LogFiles
    (log-files [_ test node] [log-file])
  )
)
