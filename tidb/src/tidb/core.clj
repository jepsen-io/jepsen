(ns tidb.core
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
  	        [clojure.string :as str]
            [jepsen
              [tests :as tests]
              [control :as c]
              [cli :as cli]
              [db :as db]
            ]
            [jepsen.control.util :as cu]
  )
)

(def tidb-url "http://download.pingcap.org/tidb-latest-linux-amd64.tar.gz")
(def tidb-dir "/opt/tidb")
(def tipd "./bin/pd-server")
(def tikv "./bin/tikv-server")
(def tidb "./bin/tidb-server")
(def pdlogfile (str tidb-dir "/jepsen-pd.log"))
(def pdpidfile (str tidb-dir "/jepsen-pd.pid"))
(def kvlogfile (str tidb-dir "/jepsen-kv.log"))
(def kvpidfile (str tidb-dir "/jepsen-kv.pid"))
(def dblogfile (str tidb-dir "/jepsen-db.log"))
(def dbpidfile (str tidb-dir "/jepsen-db.pid"))

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

(defn pd-cluster
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

        ; ./bin/pd-server --name=pd1 \
        ;                 --data-dir=pd1 \
        ;                 --client-urls="http://n1:2379" \
        ;                 --peer-urls="http://n1:2380" \
                        ; --initial-cluster="pd1=http://n1:2380, \
                                           ; pd2=http://n2:2380, \
                                           ; pd3=http://n3:2380" \
                                           ; pd4=http://n4:2380" \
                                           ; pd5=http://n5:2380" \
        ;                 --log-file=pd.log
        (info node "starting pd-server")
        (cu/start-daemon!
          {:logfile pdlogfile
           :pidfile pdpidfile
           :chdir   tidb-dir
          }
          tipd
          :--name            (get-in tidb-map [node :pd])
          :--data-dir        (get-in tidb-map [node :pd])
          :--client-urls     (client-url node)
          :--peer-urls       (peer-url node)
          :--initial-cluster (initial-cluster test)
          :--log-file        (str "pd.log")
        )

        ; ./bin/tikv-server --pd="n1:2379,n2:2379,n3:2379,n4:2379,n5:2379" \
                          ; --addr="n1:20160" \
                          ; --data-dir=tikv1 \
                          ; --log-file=tikv.log
        (info node "starting tikv-server")
        (cu/start-daemon!
          {:logfile kvlogfile
           :pidfile kvpidfile
           :chdir   tidb-dir
          }
          tikv
          :--pd        (pd-cluster test)
          :--addr      (str (name node) ":" "20160")
          :--data-dir  (get-in tidb-map [node :kv])
          :--log-file  (str "tikv.log")
        )

        ; ./bin/tidb-server --store=tikv \
                          ; --path="n1:2379,n2:2379,n3:2379,n4:2379,n5:2379" \
                          ; --log-file=tidb.log
        (info node "starting tidb-server")
        (cu/start-daemon!
          {:logfile dblogfile
           :pidfile dbpidfile
           :chdir   tidb-dir
          }
          tidb
          :--store     (str "tikv")
          :--path      (pd-cluster test)
          :--log-file  (str "tidb.log")
        )

        (Thread/sleep 5000)
      )
    )
    (teardown! [_ test node]
      (info node "tearing down TiDB")
      (cu/stop-daemon! tidb dbpidfile)
      (cu/stop-daemon! tikv kvpidfile)
      (cu/stop-daemon! tipd pdpidfile)
      (c/su (c/exec :rm :-rf tidb-dir))
    )
  )
)

(defn tidb-test
  [opts]
    (merge tests/noop-test
      {:name "TiDB"
       :db (db opts)
      }
      opts
    )
)

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn tidb-test}) args)
)
