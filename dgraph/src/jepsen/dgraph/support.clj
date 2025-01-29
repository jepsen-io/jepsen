(ns jepsen.dgraph.support
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [clj-http.client :as http]
            [clojure.pprint :refer [pprint]]
            [cheshire.core :as json]
            [dom-top.core :refer [assert+ with-retry]]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen [db       :as db]
                    [control  :as c]
                    [core     :as jepsen]
                    [util     :refer [meh]]]
            [jepsen.nemesis.time :as nt]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.dgraph.client :as dc]
            [jepsen.dgraph.trace :as t]
            [clojure.java.io :as io])
  (:import (java.util.concurrent CyclicBarrier)
           (java.io File)))

; Local paths
(def dir "/opt/dgraph")
(def binary "dgraph")
(def ratel-binary "dgraph-ratel")
(def zero-logfile  (str dir "/zero.log"))
(def alpha-logfile (str dir "/alpha.log"))
(def ratel-logfile (str dir "/ratel.log"))
(def zero-pidfile  (str dir "/zero.pid"))
(def alpha-pidfile (str dir "/alpha.pid"))
(def ratel-pidfile (str dir "/ratel.pid"))

; Ports
(def zero-internal-port 5080)
(def zero-public-port   6080)
(def internal-port      7080)
(def public-port        8080)
(def public-grpc-port   9080)

(def alpha-port-offset  0)
(def zero-port-offset   0)
(def ratel-port-offset  2)

(def alpha-internal-port  (+ internal-port      alpha-port-offset))
(def zero-internal-port   (+ zero-internal-port zero-port-offset))
(def ratel-internal-port  (+ internal-port      ratel-port-offset))
(def alpha-public-port    (+ public-port        alpha-port-offset))
(def zero-public-port     (+ zero-public-port   zero-port-offset))
(def ratel-public-port    (+ public-port        ratel-port-offset))
(def alpha-public-grpc-port (+ public-grpc-port alpha-port-offset))

(defn node-idx
  "Given a node and a test, returns the index for that particular node, based
  on its position in the test's list of nodes."
  [test node]
  ; Indices should start with 1
  (inc (.indexOf (:nodes test) node)))

(defn start-zero!
  "Launch dgraph zero on a node"
  [test node]
  (c/su (cu/start-daemon!
          {:logfile zero-logfile
           :pidfile zero-pidfile
           :chdir   dir}
          binary
          :zero
          :--idx                (node-idx test node)
          :--port_offset        zero-port-offset
          :--expose_trace
          :--v 2
          :--replicas           (:replicas test)
          :--rebalance_interval (:rebalance-interval test)
          (when (:dgraph-jaeger-collector test)
            [:--jaeger.collector (:dgraph-jaeger-collector test)])
          :--my                 (str node ":" zero-internal-port)
          (when-not (= node (jepsen/primary test))
            [:--peer (str (jepsen/primary test) ":" zero-internal-port)])))
  :started)

(defn lru-opt
  "Between 1.0.4 and 1.0.5, Dgraph changed the name of their mandatory LRU
  memory option and there's no option that works across build, so we have to
  detect which version of the option name to use from the help in order to test
  different builds."
  []
  (let [usage (c/exec (str dir "/" binary) :alpha :--help)
        opt (re-find #"(--(lru|memory)_mb)" usage)]
    (assert+ opt RuntimeException
             (str "Not sure whether to use --lru_mb or --memory_mb with "
                  "this dgraph build. It told me:\n\n" usage))
    [(opt 1) 1024]))

(defn start-alpha!
  "Launch dgraph data server on a node."
  [test node]
  (c/su (c/cd dir (c/exec (str "./" binary) :version :>> alpha-logfile))
        (cu/start-daemon!
          {:logfile alpha-logfile
           :pidfile alpha-pidfile
           :chdir   dir}
          binary
          :alpha
          (lru-opt)
          :--expose_trace
          :--v 2
          :--vmodule=groups=3 ;; flag to set -v=3 for worker/groups.go
          (when (:dgraph-jaeger-collector test)
            [:--jaeger.collector (:dgraph-jaeger-collector test)])
          (when (:dgraph-jaeger-agent test)
            [:--jaeger.agent (:dgraph-jaeger-agent test)])
          :--idx        (node-idx test node)
          :--my         (str node ":" alpha-internal-port)
          :--zero       (str node ":" zero-internal-port)))
  :started)

(defn start-ratel!
  "Launch dgraph UI server on a node."
  [test node]
  (c/su (cu/start-daemon!
          {:logfile ratel-logfile
           :pidfile ratel-pidfile
           :chdir   dir}
          ratel-binary
          :-addr (str node ":" alpha-public-port)
          :-port ratel-public-port))
  :started)

(defn stop-zero!
  "Kills zero"
  [test node]
  (c/su (cu/stop-daemon! zero-pidfile))
  :stopped)

(defn stop-alpha!
  "Kills alpha"
  [test node]
  (c/su (cu/stop-daemon! alpha-pidfile))
  :stopped)

(defn stop-ratel!
  "Kills ratel"
  [test node]
  (c/su (cu/stop-daemon! ratel-pidfile))
  :stopped)

(def http-opts
  "Default clj-http options"
  {:socket-timeout 1000
   :conn-timeout 1000
   :throw-exceptions? true
   :throw-entire-message? true})

(defn zero-url
  "Takes a zero node and path fragmnets, and constructs a URL for it."
  [node & path]
  (str "http://" node ":" zero-public-port "/" (str/join "/" path)))

(defn zero-state
  "Fetches zero /state from the given node."
  [node]
  (try
    (-> (http/get (zero-url node "state")
                  http-opts)
        :body
        (json/parse-string (fn [k] (if (re-find #"\A\d+\z" k)
                                     (Long/parseLong k)
                                     (keyword k)))))
    ;; It's ok if this times out, just move on
    (catch java.net.SocketTimeoutException e :timeout)))

(defn zero-leader
  "Takes result of zero-state and returns the node of the zero leader"
  [state]
  (let [leader (->> state
                    :zeros
                    vals
                    (filter :leader)
                    first
                    :addr)]
    (first (clojure.string/split leader #":"))))

(defn move-tablet!
  "Given a zero node, asks that node to move a tablet to the given group."
  [node tablet group]
  (t/with-trace "support.move-tablet!"
    (-> (http/get (zero-url node "moveTablet")
                  (assoc http-opts
                         :socket-timeout 20000
                         :query-params {:tablet tablet
                                        :group  group}))
        :body)))

(defn cluster-ready?
  "Does this zero node think we're ready to start work?"
  [node test]
  (try
    (let [s       (zero-state node)
          indexen (->> (:nodes test)
                       (map (partial node-idx test))
                       set)
          addrs   (->> (:nodes test)
                       (map #(str % ":" alpha-internal-port))
                       set)]
      ; We need all zero nodes to be up
      (and (= indexen (set (keys (:zeros s))))
           ; We want every alpha node to be serving something
           (->> (:groups s)
                vals
                (mapcat (fn [group] (map :addr (vals (:members group)))))
                set
                (= addrs))))
    (catch java.net.SocketTimeoutException e
      false)))

(defn wait-for-cluster
  "Blocks until this Zero indicates the cluster is ready to go."
  [node test]
  (loop [attempts 20]
    (or (cluster-ready? node test)
        (do (when (<= attempts 1)
              (throw+ {:type        :cluster-failed-to-converge
                       :node        node
                       :zero-state  (meh (zero-state node))}))
            (info "Waiting for cluster convergence")
            (Thread/sleep 1000)
            (recur (dec attempts))))))

(defn db
  "Sets up dgraph. Opts should include:

    :nodes        A collection of node names

  Test should include

  :version      Version to install e.g. 1.0.2
  :replicas     How many copies of each group to store"
  [opts]
  ; Ugh this is such a hack. We need a place to synchronize before exiting on
  ; teardown, but only AFTER the initial teardown we do as a part of setup. So
  ; we build a CyclicBarrier here. Because we have to run after setup, we
  ; *also* use a has-setup? atom which gets set to true as a part of the setup
  ; process.
  (let [teardown-barrier (CyclicBarrier. (count (:nodes opts)))
        has-setup?       (atom false)]
    (reify db/DB
      (setup! [_ test node]
        (c/su
         (if-let [file (:local-binary test)]
           (do ; Upload local file
             (c/exec :mkdir :-p dir)
             (info "Uploading" file "...")
             (c/upload file (str dir "/" binary))
             (c/exec :chmod :+x (str dir "/" binary)))
           ;; Install remote package
           (cu/install-archive!
            (or (:package-url test)
                (str "https://github.com/dgraph-io/dgraph/releases/download/v"
                     (:version test) "/dgraph-linux-amd64.tar.gz"))
            dir
            (:force-download test)))

         (nt/install!)

         (when (= node (jepsen/primary test))
           (start-zero! test node)
           (Thread/sleep 10000))

         (jepsen/synchronize test 120)
         (when-not (= node (jepsen/primary test))
           (start-zero! test node))

         (jepsen/synchronize test)
         (start-alpha! test node)
         ;; (start-ratel! test node)

         (try+
          (when (= node (jepsen/primary test))
            (wait-for-cluster node test)
            (info "Cluster converged"))

          (jepsen/synchronize test 300)
          (with-retry [attempts 4]
            (try
              (dc/unwrap-exceptions
                (let [conn (dc/open node alpha-public-grpc-port)]
                  (try (dc/await-ready conn)
                       (finally
                         (dc/close! conn)))))

              (catch io.grpc.StatusRuntimeException e
                (if (re-find #"error while fetching schema" (.getMessage e))
                  (do
                    (info "Error fetching schema, retrying.")
                    (Thread/sleep 5000)
                    (retry (dec attempts)))
                  (throw+ e)))))

          (info "GRPC ready")

          (catch [:type :cluster-failed-to-converge] e
            (when-not (:retry-db-setup test)
              (throw+))

            (warn e "Cluster failed to converge")
            (throw (ex-info "Cluster failed to converge"
                            {:type  :jepsen.db/setup-failed
                             :node  node}
                            (:throwable &throw-context))))

          (catch RuntimeException e ; Welp
            (when-not (:retry-db-setup test)
              (throw+))

            (throw (ex-info "Couldn't get a client"
                            {:type  :jepsen.db/setup-failed
                             :node  node}
                            e)))))
         (reset! has-setup? true))

      (teardown! [this test node]
        (when (:defer-db-teardown test)
          (when @has-setup?
            (when (= (jepsen/primary test) node)
              (info "Waiting to tear down nodes; hit [enter] to tear down.")
              (read-line))
            (.await teardown-barrier)))

        (stop-ratel! test node)
        (stop-alpha! test node)
        (stop-zero! test node)
        (c/su
         (c/exec :rm :-rf dir)))

      db/LogFiles
      (log-files [_ test node]
        (let [tmp-dir "/tmp/jepsen-dgraph-data"
              tarball (str dir "/data.tar.bz2")]
          (c/su (c/exec :rm :-rf tmp-dir)
                (c/exec :mkdir :-p tmp-dir)
                (doseq [d ["p" "w" "zw"]]
                  (meh (c/exec :cp :-r (str dir "/" d) tmp-dir)))
                (c/exec :tar :cjf tarball tmp-dir))
          [alpha-logfile
           zero-logfile
           tarball])))))
