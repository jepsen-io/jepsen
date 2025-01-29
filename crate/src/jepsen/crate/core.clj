(ns jepsen.crate.core
  (:require [jepsen [core         :as jepsen]
                    [db           :as db]
                    [control      :as c :refer [|]]
                    [checker      :as checker]
                    [cli          :as cli]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [net          :as net]
                    [store        :as store]
                    [tests        :as tests]
                    [util         :as util :refer [meh
                                                   timeout
                                                   with-retry]]
                    [os           :as os]
                    [reconnect :as rc]]
            [jepsen.os.debian     :as debian]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util  :as cu]
            [jepsen.control.net   :as cnet]
            [cheshire.core        :as json]
            [clojure.string       :as str]
            [clojure.java.jdbc    :as j]
            [clojure.java.io      :as io]
            [clojure.java.shell   :refer [sh]]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [clj-http.client          :as http]
            [knossos.op           :as op])
  (:import (java.net InetAddress)
           (io.crate.shade.org.postgresql.util PSQLException)
           (org.elasticsearch.rest RestStatus)
           (org.elasticsearch.common.unit TimeValue)
           (org.elasticsearch.common.settings
             Settings)
           (org.elasticsearch.common.transport
             InetSocketTransportAddress)
           (org.elasticsearch.client.transport
             TransportClient)
           (org.elasticsearch.transport.client
             PreBuiltTransportClient)))

(def timeout-delay "Default timeout for operations in ms" 10000)
(def max-timeout "Longest timeout, in ms" 30000)

(def user "crate")
(def base-dir "/opt/crate")
(def pidfile "/tmp/crate.pid")
(def stdout-logfile (str base-dir "/logs/stdout.log"))

(defn map->kw-map
  "Turns any map into a kw-keyed persistent map."
  [x]
  (condp instance? x
    java.util.Map
    (reduce (fn [m pair]
              (assoc m (keyword (key pair)) (val pair)))
            {}
            x)

    java.util.List
    (map map->kw-map x)

    true
    x))

;; ES client

(defn ^TransportClient es-connect-
  "Open an elasticsearch connection to a node."
  [node]
  (..  (PreBuiltTransportClient.
         (.. (Settings/builder)
             (put "cluster.name" "crate")
             (put "client.transport.sniff" false)
             (build))
         (make-array Class 0))
      (addTransportAddress (InetSocketTransportAddress.
                             (InetAddress/getByName (name node)) 44300))))

(defn es-connect
  "Opens an ES connection to a node, and ensures that it actually works"
  [node]
  (let [c (es-connect- node)]
    (util/with-retry [i 10]
      (-> c
          (.admin)
          (.cluster)
          (.prepareState)
          (.all)
          (.execute)
          (.actionGet))
      c
      (catch org.elasticsearch.client.transport.NoNodeAvailableException e
        (when (zero? i)
          (throw e))
        (info "Client not ready:" (type e))
        (Thread/sleep 5000)
        (retry (dec i))))))

(defn es-index!
  "Index a record"
  [^TransportClient client index type doc]
  (assert (:id doc))
  (let [res (-> client
                (.prepareIndex index type (str (:id doc)))
                (.setSource (json/generate-string doc))
                (.get))]; why not execute/actionGet?
    (when-not (= RestStatus/CREATED (.status res))
      (throw (RuntimeException. "Document not created")))
    res))

(defn es-get
  "Get a record by ID. Returns nil when record does not exist."
  [^TransportClient client index type id]
  (let [res (-> client
                (.prepareGet index type (str id))
                (.get))]
    (when (.isExists res)
      {:index   (.getIndex res)
       :type    (.getType res)
       :id      (.getId res)
       :version (.getVersion res)
       :source  (map->kw-map (.getSource res))})))

(defn es-search
  [^TransportClient client]
  (loop [results []
         scroll  (-> client
                     (.prepareSearch (into-array String []))
                     (.setScroll (TimeValue. 60000))
                     (.setSize 128)
                     (.execute)
                     (.actionGet))]
    (let [hits (.getHits (.getHits scroll))]
      (if (zero? (count hits))
        ; Done
        results

        (recur
          (->> hits
               seq
               (map (fn [hit]
                      {:id      (.id hit)
                       :version (.version hit)
                       :source  (map->kw-map (.getSource hit))}))
               (into results))
          (-> client
              (.prepareSearchScroll (.getScrollId scroll))
              (.setScroll (TimeValue. 60000))
              (.execute)
              (.actionGet)))))))

(def cratedb-spec {:dbtype "crate"
                   :dbname "test"
                   :classname "io.crate.client.jdbc.CrateDriver"
                   :user "crate"
                   :password ""
                   :port 55432})

(defn get-node-db-spec
  "Creates the db spec for the provided node"
  [node]
  (merge cratedb-spec {:host (name node)}))

(defn close-conn
  "Given a JDBC connection, closes it and returns the underlying spec."
  [conn]
  (when-let [c (j/db-find-connection conn)]
    (.close c))
  (dissoc conn :connection))

(defn jdbc-client
  "Constructs a jdbc client for a node, and opens it"
  [node]
  (rc/open!
    (rc/wrapper
      {:name [:crate node]
       :open (fn open []
               (util/timeout max-timeout
                             (throw (RuntimeException.
                                      (str "Connection to " node " timed out")))
                             (let [spec (get-node-db-spec node)
                                   conn (j/get-connection spec)
                                   spec' (j/add-connection spec conn)]
                               (assert spec')
                               spec')))
       :close close-conn
       :log? true})))

(defmacro with-conn
  "Like jepsen.reconnect/with-conn, but also asserts that the connection has
  not been closed. If it has, throws an ex-info with :type :conn-not-ready.
  Delays by 1 second to allow time for the DB to recover."
  [[c client] & body]
  `(rc/with-conn [~c ~client]
     (when (.isClosed (j/db-find-connection ~c))
       (Thread/sleep 1000)
       (throw (ex-info "Connection not yet ready."
                       {:type :conn-not-ready})))
     ~@body))

(defmacro with-timeout
  "Like util/timeout, but throws (RuntimeException. \"timeout\") for timeouts.
  Throwing means that when we time out inside a with-conn, the connection state
  gets reset, so we don't accidentally hand off the connection to a later
  invocation with some incomplete transaction."
  [& body]
  `(util/timeout timeout-delay
                 (throw (RuntimeException. "timeout"))
                 ~@body))

(defmacro with-txn
  "Wrap a evaluation within a SQL transaction."
  [[c conn] & body]
  `(j/with-db-transaction [~c ~conn]
     ~@body))

(defmacro with-exception->op
  "Takes an operation and a body. Evaluates body, catches exceptions, and maps
  them to ops with :type :info and a descriptive :error."
  [op & body]
  `(try ~@body
        (catch Exception e#
          (if-let [ex-op# (exception->op e#)]
            (merge ~op ex-op#)
            (throw e#)))))

(defn query
  "Like jdbc query, but includes a default timeout in ms."
  ([conn expr]
   (query conn expr {}))
  ([conn [sql & params] opts]
   (let [s (j/prepare-statement (j/db-find-connection conn)
                                sql
                                {:timeout (/ timeout-delay 1000)})]
     (try
       (j/query conn (into [s] params) opts)
       (finally
         (.close s))))))

(defn wait
  "Waits for crate to be healthy on the current node. Color is red,
  yellow, or green; timeout is in seconds."
  [node timeout-secs color]
  (timeout (* 1000 timeout-secs)
           (throw (RuntimeException.
                    (str "Timed out after "
                         timeout-secs
                         " s waiting for crate cluster recovery")))
           (util/with-retry []
             (-> (str "http://" (name node) ":44200/_cluster/health/?"
                      "wait_for_status=" (name color)
                      "&timeout=" timeout-secs "s")
                 (http/get {:as :json})
                 :status
                 (= 200)
                 (or (retry)))
             (catch java.io.IOException e
               (retry)))))

;; DB

(defn install-open-jdk8!
  "Installs open jdk8"
  []
  (c/su
    (debian/add-repo!
      "backports"
      "deb http://http.debian.net/debian jessie-backports main")
      (c/exec :apt-get :update)
      (c/exec :apt-get :install :-y :-t :jessie-backports "openjdk-8-jdk")
      (c/exec :update-java-alternatives :--set "java-1.8.0-openjdk-amd64")
    ))

(defn install!
  "Install crate."
  [node tarball-url]
  (c/su
    (debian/install [:apt-transport-https])
    (install-open-jdk8!)
    (cu/ensure-user! user)
    (cu/install-archive! tarball-url base-dir false)
    (c/exec :chown :-R (str user ":" user) base-dir))
  (info node "crate installed"))

(defn majority
  "n/2+1"
  [n]
  (-> n (/ 2) inc Math/floor long))

(defn configure!
  "Set up config files."
  [node test]
  (c/su
    (c/exec :echo
            (-> "crate.yml"
                io/resource
                slurp
                (str/replace "$NAME" (name node))
                (str/replace "$HOST" (.getHostAddress
                                       (InetAddress/getByName (name node))))
                (str/replace "$N" (str (count (:nodes test))))
                (str/replace "$MAJORITY" (str (majority (count (:nodes test)))))
                (str/replace "$UNICAST_HOSTS"
                             (clojure.string/join ", " (map (fn [node]
                                                              (str "\"" (name node) ":44300\"" ))
                                                            (:nodes test))))
                )
            :> (str base-dir "/config/crate.yml"))

      (c/exec :echo
            (-> "crate.in.sh"
                io/resource
                slurp)
            :> (str base-dir "/bin/crate.in.sh")))
  (info node "configured"))

(defn start!
  [node]
  (info node "starting crate")
  (c/su (c/exec :sysctl :-w "vm.max_map_count=262144"))
  (c/cd base-dir
        (c/sudo user
                (c/exec :mkdir :-p (str base-dir "/logs"))
                (cu/start-daemon!
                  {:logfile stdout-logfile
                   :pidfile pidfile
                   :chdir   base-dir}
                  "bin/crate")))
  (wait node 90 :green)
  (info node "crate started"))

(defn db
  [tarball-url]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! tarball-url)
        (configure! test)
        (start!)))

    (teardown! [_ test node]
      (cu/grepkill! "crate")
      (info node "killed")
      (c/exec :rm :-rf (c/lit (str base-dir "/logs/*")))
      (c/exec :rm :-rf (c/lit (str base-dir "/data/*"))))

    db/LogFiles
    (log-files [_ test node]
      [(str base-dir "/logs/crate.log")])))

(defmacro with-errors
  "Unified error handling: takes an operation, evaluates body in a try/catch,
  and maps common exceptions to short errors."
  [op & body]
  `(try ~@body
        (catch PSQLException e#
          (cond
            (and (= 0 (.getErrorCode e#))
                 (re-find #"blocked by: \[.+no master\];" (str e#)))
            (assoc ~op :type :fail, :error :no-master)

            (and (= 0 (.getErrorCode e#))
                 (re-find #"document with the same primary key" (str e#)))
            (assoc ~op :type :fail, :error :duplicate-key)

            (and (= 0 (.getErrorCode e#))
                 (re-find #"rejected execution" (str e#)))
            (do ; Back off a bit
                (Thread/sleep 1000)
                (assoc ~op :type :info, :error :rejected-execution))

            :else
            (throw e#)))))

(defn exception->op
  "Takes an exception and maps it to a partial op, like {:type :info, :error
  ...}. nil if unrecognized."
  [e]
  (when-let [m (.getMessage e)]
    (condp instance? e
      java.sql.SQLTransactionRollbackException
      {:type :fail, :error [:rollback m]}

      java.sql.BatchUpdateException
      (if (re-find #"getNextExc" m)
        ; Wrap underlying exception error with [:batch ...]
        (when-let [op (exception->op (.getNextException e))]
          (update op :error (partial vector :batch)))
        {:type :info, :error [:batch-update m]})

      PSQLException
      (condp re-find (.getMessage e)
        #"Connection .+? refused"
        {:type :fail, :error :connection-refused}

        #"context deadline exceeded"
        {:type :fail, :error :context-deadline-exceeded}

        #"rejecting command with timestamp in the future"
        {:type :fail, :error :reject-command-future-timestamp}

        #"encountered previous write with future timestamp"
        {:type :fail, :error :previous-write-future-timestamp}

        #"restart transaction"
        {:type :fail, :error [:restart-transaction m]}

        {:type :info, :error [:psql-exception m]})

      clojure.lang.ExceptionInfo
      (condp = (:type (ex-data e))
        :conn-not-ready {:type :fail, :error :conn-not-ready}
        nil)

      (condp re-find m
        #"^timeout$"
        {:type :info, :error :timeout}
        nil))))
