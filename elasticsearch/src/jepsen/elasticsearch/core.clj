(ns jepsen.elasticsearch.core
  (:require [cheshire.core          :as json]
            [clojure.java.io        :as io]
            [clojure.string         :as str]
            [clojure.tools.logging  :refer [info]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.checker.timeline  :as timeline]
            [jepsen.control.net       :as cnet]
            [jepsen.control.util      :as cu]
            [jepsen.os.debian         :as debian]
            [clj-http.client          :as http])
  (:import (java.net InetAddress)
           ; This is how the non-rest client communicates response status (!?)
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

(def user "elasticsearch")
(def base-dir "/opt/elasticsearch")
(def pidfile "/tmp/elasticsearch.pid")
(def stdout-logfile (str base-dir "/logs/stdout.log"))
(def cluster-name "jepsen")
(def cluster-logs (map (partial str base-dir "/logs/")
                       (cons (str cluster-name ".log")
                             (map (partial str cluster-name "_")
                                  ["deprecation.log"
                                   "index_indexing_slowlog.log"
                                   "index_search_slowlog.log"]))))
(def logs (cons stdout-logfile cluster-logs))

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
             (put "cluster.name" cluster-name)
             (put "client.transport.sniff" false)
             (build))
         (make-array Class 0))
      (addTransportAddress (InetSocketTransportAddress.
                             (InetAddress/getByName (name node)) 9300))))

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

(defn http-error
  "Takes an elastisch ExInfo exception and extracts the HTTP error response as
  a string."
  [ex]
  ; AFIACT this is the shortest path to actual information about what went
  ; wrong
  (-> ex .getData :body json/parse-string (get "error")))

(defn wait
  "Waits for elasticsearch to be healthy on the current node. Color is red,
  yellow, or green; timeout is in seconds."
  [node timeout-secs color]
  (timeout (* 1000 timeout-secs)
           (throw (RuntimeException.
                    (str "Timed out after "
                         timeout-secs
                         " s waiting for elasticsearch cluster recovery")))
           (util/with-retry []
             (-> (str "http://" (name node) ":9200/_cluster/health/?"
                      "wait_for_status=" (name color)
                      "&timeout=" timeout-secs "s")
                 (http/get {:as :json})
                 :status
                 (= 200)
                 (or (retry)))
             (catch java.io.IOException e
               (retry)))))

(defn primaries
  "Returns a map of nodes to the node that node thinks is the current primary,
  as a map of keywords to keywords. Assumes elasticsearch node names are the
  same as the provided node names."
  [nodes]
  (->> nodes
       (pmap (fn [node]
               (try
                 (let [res (-> (str "http://" (name node)
                                    ":9200/_cluster/state")
                               (http/get {:as :json-string-keys})
                               :body)
                       primary (get res "master_node")]
                   [node
                    (keyword (get-in res ["nodes" primary "name"]))])
                 (catch java.net.ConnectException e [node nil])
                 (catch clojure.lang.ExceptionInfo e
                   (when-not (re-find #"MasterNotDiscoveredException"
                                      (http-error e))
                     (throw e))
                   [node nil]))))
       (into {})))

(defn self-primaries
  "A sequence of nodes which think they are primaries."
  [nodes]
  (->> nodes
       primaries
       (filter (partial apply =))
       (map key)))

(defn install!
  "Install elasticsearch"
  [node tarball-url]
  (c/su
    (debian/install-jdk8!)
    (cu/ensure-user! user)
    (cu/install-tarball! node tarball-url base-dir false)
    (c/exec :chown :-R (str user ":" user) base-dir)))

(defn configure!
  "Configures elasticsearch."
  [node test]
  (c/su
    (c/exec :echo
            (-> "elasticsearch.yml"
                io/resource
                slurp
                (str/replace "$CLUSTER" cluster-name)
                (str/replace "$NAME" (name node))
                (str/replace "$N" (str (count (:nodes test))))
                (str/replace "$MAJORITY" (str (util/majority
                                                (count (:nodes test)))))
                (str/replace "$HOSTS"
                             (json/generate-string
                               (vals (c/on-nodes test (fn [_ _]
                                                        (cnet/local-ip)))))))
            :> (str base-dir "/config/elasticsearch.yml"))

    (c/exec :echo
            (-> "jvm.options"
                io/resource
                slurp)
            :> (str base-dir "/config/jvm.options")))
  (info node "configured"))

(defn start!
  "Starts elasticsearch"
  [node]
  (info node "starting elasticsearch")
  (c/su
    (c/exec :sysctl :-w "vm.max_map_count=262144"))
  (c/cd base-dir
        (c/sudo user
                (c/exec :mkdir :-p (str base-dir "/logs"))
                (cu/start-daemon! {:logfile stdout-logfile
                                   :pidfile pidfile
                                   :chdir   base-dir}
                                  "bin/elasticsearch")))
  (wait node 60 :green)
  (info node "elasticsearch ready"))

(defn stop!
  "Shuts down elasticsearch"
  [node]
  (c/su
    (cu/stop-daemon! :java pidfile))
  (info node "elasticsearch stopped"))

(defn nuke!
  "Shuts down server and destroys all data."
  [node]
  (stop! node)
  (c/su
    (c/exec :rm :-rf (c/lit (str base-dir "/data/*")))
    (doseq [l logs]
      (meh
        (c/exec :truncate :--size 0 l))))
  (info node "elasticsearch nuked"))

(defn db
  "Elasticsearch for a particular version."
  [tarball-url]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! tarball-url)
        (configure! test)
        (start!)))

    (teardown! [_ test node]
      (nuke! node))

    db/LogFiles
    (log-files [_ test node]
      logs)))

(defn index-already-exists-error?
  "Return true if the error is due to the index already existing, false
  otherwise."
  [error]
  (and
   (-> error .getData :status (= 400))
   (re-find #"IndexAlreadyExistsException"
            (http-error error))))

;(defn await-client
;  "Opens an elasticsearch client. Spins for up to 60s waiting for a
;  connection."
;  [node]
;  (let [client (es/connect (str "http://" (name node) ":9200"))
;        err (atom (RuntimeException. "Client conn timed out"))]
;    (util/timeout 60000 (throw @err)
;      (util/with-retry []
;        (esi/exists? client "test")
;        client
;        (catch Exception e
;          (reset! err e)
;          (retry))))))

; Nemeses

(defn mostly-small-nonempty-subset
  "Returns a subset of the given collection, with a logarithmically decreasing
  probability of selecting more elements. Always selects at least one element.

      (->> #(mostly-small-nonempty-subset [1 2 3 4 5])
           repeatedly
           (map count)
           (take 10000)
           frequencies
           sort)
      ; => ([1 3824] [2 2340] [3 1595] [4 1266] [5 975])"
  [xs]
  (-> xs
      count
      inc
      Math/log
      rand
      Math/exp
      long
      (take (shuffle xs))))

(def isolate-self-primaries-nemesis
  "A nemesis which completely isolates any node that thinks it is the primary."
  (nemesis/partitioner
    (fn [nodes]
      (let [ps (self-primaries nodes)]
        (nemesis/complete-grudge
          ; All nodes that aren't self-primaries in one partition
          (cons (remove (set ps) nodes)
                ; Each self-primary in a different partition
                (map list ps)))))))

(def crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  (nemesis/node-start-stopper
    mostly-small-nonempty-subset
    (fn start [test node] (c/su (c/exec :killall :-9 :java)) [:killed node])
    (fn stop  [test node] (start! node) [:restarted node])))

(def crash-primary-nemesis
  "A nemesis that crashes a random primary node."
  (nemesis/node-start-stopper
    (comp rand-nth self-primaries)
    (fn start [test node] (c/su (c/exec :killall :-9 :java)) [:killed node])
    (fn stop  [test node] (start! node) [:restarted node])))
