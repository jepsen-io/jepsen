(ns elasticsearch.core
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
            [jepsen.control.net       :as net]
            [jepsen.os.debian         :as debian]
            [clj-http.client          :as http]
            [clojurewerkz.elastisch.rest          :as es]
            [clojurewerkz.elastisch.rest.admin    :as esa]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.index    :as esi]
            [clojurewerkz.elastisch.rest.response :as esr]))

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
  [timeout-secs color]
  (timeout (* 1000 timeout-secs)
           (throw (RuntimeException.
                    (str "Timed out after "
                         timeout-secs
                         " s waiting for elasticsearch cluster recovery")))
    (loop []
      (when
        (try
          (c/exec :curl :-XGET
                  (str "http://localhost:9200/_cluster/health?"
                       "wait_for_status=" (name color)
                       "&timeout=" timeout-secs "s"))
          false
          (catch RuntimeException e true))
        (recur)))))

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
  "Install elasticsearch!"
  [node version]
  (c/su
    (c/cd "/tmp"
          (let [debfile (str "elasticsearch-" version ".deb")
                uri     (str "https://download.elasticsearch.org/"
                             "elasticsearch/elasticsearch/"
                             debfile)]
            (when-not (= version (debian/installed-version "elasticsearch"))
              (debian/uninstall! ["elasticsearch"])

              (loop []
                (info node "downloading elasticsearch")
                (c/exec :wget :-c uri)
                (c/exec :wget :-c (str uri ".sha1.txt"))
                (when (try
                        (c/exec :sha1sum :-c (str debfile ".sha1.txt"))
                        false
                        (catch RuntimeException e
                          (info "SHA failed" (.getMessage e))
                          true))
                  (recur)))

              (info node "installing elasticsearch")
              (debian/install ["openjdk-7-jre-headless"])
              (c/exec :dpkg :-i :--force-confnew debfile))))))

(defn configure!
  "Configures elasticsearch."
  [node test]
  (c/su
    (info node "configuring elasticsearch")
    (c/exec :echo
            (-> "default"
                io/resource
                slurp)
            :> "/etc/default/elasticsearch")
    (c/exec :echo
            (-> "elasticsearch.yml"
                io/resource
                slurp
                (str/replace "$NAME"  (name node))
                (str/replace "$HOSTS" (json/generate-string
                                        (vals (c/on-many (:nodes test)
                                                         (net/local-ip))))))
            :> "/etc/elasticsearch/elasticsearch.yml")))

(defn start!
  [node]
  "Starts elasticsearch."
  (info node "starting elasticsearch")
  (c/su (c/exec :service :elasticsearch :restart))

  (wait 60 :green)
  (info node "elasticsearch ready"))

(defn stop!
  "Shuts down elasticsearch."
  [node]
  (c/su
    (meh (c/exec :service :elasticsearch :stop))
    (meh (c/exec :killall :-9 :elasticsearch)))
  (info node "elasticsearch stopped"))

(defn nuke!
  "Shuts down server and destroys all data."
  [node]
  (stop! node)
  (c/su
    (c/exec :rm :-rf "/var/lib/elasticsearch/elasticsearch")
    (meh
      (c/exec :truncate :--size 0 "/var/log/elasticsearch/elasticsearch.log")))
  (info node "elasticsearch nuked"))

(defn db
  "Elasticsearch for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (nuke!)
        (install! version)
        (configure! test)
        (start!)))

    (teardown! [_ test node]
      ;; Leave system up, to collect logs, analyze post mortem, etc
      )))

(def index-name "jepsen-index")

(defn index-already-exists-error?
  "Return true if the error is due to the index already existing, false
  otherwise."
  [error]
  (and
   (-> error .getData :status (= 400))
   (re-find #"IndexAlreadyExistsException"
            (http-error error))))

(defrecord CreateSetClient [client]
  client/Client
  (setup! [_ test node]
    (let [; client (es/connect [[(name node) 9300]])]
          client (es/connect (str "http://" (name node) ":9200"))]
      ;; Create index
      (try
        (esi/create client index-name
                    :mappings {"number" {:properties
                                         {:num {:type "integer"
                                                :store "yes"}}}}
                    :settings {"index" {"refresh_interval" "1s"}})
        (catch clojure.lang.ExceptionInfo e
          (when-not (index-already-exists-error? e)
            (throw e))))
      (esa/cluster-health client
                          {:index [index-name] :level "indices"
                           :wait_for_status "green"
                           :wait_for_nodes (count (:nodes test))})
      (CreateSetClient. client)))

  (invoke! [this test op]
      (case (:f op)
        :add (timeout 5000
               (assoc op :type :info :value :timed-out)
               (try
                 (let [r (esd/create client index-name "number" {:num (:value op)})]
                   (cond (esr/ok? r)
                         (assoc op :type :ok)

                         (esr/timed-out? r)
                         (assoc op :type :info :value :timed-out)

                         :else
                         (assoc op :type :info :value r)))
                 (catch java.net.SocketException e
                   (assoc op :type :info :value (.getMessage e)))
                 (catch java.net.ConnectException e
                   (assoc op :type :fail :value (.getMessage e)))))
        :read (try
                (info "Waiting for recovery before read")
                ; Elasticsearch lies about cluster state during split brain
                ; woooooooo
                (c/on-many (:nodes test) (wait 1000 :green))
                (info "Elasticsearch reports green, but it's probably lying, so waiting another 10s")
                (Thread/sleep (* 10 1000))
                (info "Recovered; flushing index before read")
                (esi/flush client index-name)
                (assoc op :type :ok
                       :value (->> (esd/search client index-name "number"
                                               :scroll "10s"
                                               :size 20)
                                   (esd/scroll-seq client)
                                   (map (comp :num :_source))
                                   (into (sorted-set))))
                (catch RuntimeException e
                  (assoc op :type :fail :value (.getMessage e))))))

  (teardown! [_ test]
    (.close client)))

(defn create-set-client
  "A set implemented by creating independent documents"
  []
  (CreateSetClient. nil))

; Use ElasticSearch MVCC to do CAS read/write cycles, implementing a set.
(defrecord CASSetClient [mapping-type doc-id client]
  client/Client
  (setup! [_ test node]
    (let [; client (es/connect [[(name node) 9300]])]
          client (es/connect (str "http://" (name node) ":9200"))]
      ;; Create index
      (try
        (esi/create client index-name
                    :mappings {mapping-type {:properties {}}})
        (catch clojure.lang.ExceptionInfo e
          (when-not (index-already-exists-error? e)
            (throw e))))
      (esa/cluster-health client
                          {:index [index-name] :level "indices"
                           :wait_for_status "green"
                           :wait_for_nodes (count (:nodes test))})

      ; Initial empty set
      (esd/create client index-name mapping-type {:values []} :id doc-id)

      (CASSetClient. mapping-type doc-id client)))

  (invoke! [this test op]
    (case (:f op)
        :add (timeout 5000 (assoc op :type :info :value :timed-out)
                      (let [current (esd/get client index-name mapping-type doc-id
                                                :preference "_primary")]
                        (if-not (esr/found? current)
                          ; Can't write without a read
                          (assoc op :type :fail)

                          (let [version (:_version current)
                                values  (-> current :_source :values)
                                values' (vec (conj values (:value op)))
                                r       (esd/put client index-name mapping-type doc-id
                                                 {:values values'}
                                                 :version version)]
                            (cond ; lol esr/ok? actually means :found, and
                                  ; esr/found? means :ok or :found, and
                                  ; *neither* of those appear in a successful
                                  ; conditional put, so I hope this is how
                                  ; you're supposed to interpret responses.
                                  (esr/conflict? r)
                                  (assoc op :type :fail)

                                  (esr/timed-out? r)
                                  (assoc op :type :info :value :timed-out)

                                  :else (assoc op :type :ok))))))

        :read (try
                (info "Waiting for recovery before read")
                (c/on-many (:nodes test) (wait 200 :green))
                (info "Recovered; flushing index before read")
                (esi/flush client index-name)
                (assoc op :type :ok
                       :value (->> (esd/get client index-name mapping-type doc-id
                                       :preference "_primary")
                                   :_source
                                   :values
                                   (into (sorted-set)))))))

  (teardown! [_ test]
    (.close client)))

(defn cas-set-client []
  (CASSetClient. "cas-sets" "0" nil))

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

(defn adds
  "Generator that emits :add operations for sequential integers."
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :add, :value x}))
       gen/seq))

(defn recover
  "A generator which stops the nemesis and allows some time for recovery."
  []
  (gen/nemesis
    (gen/phases
      (gen/once {:type :info, :f :stop})
      (gen/sleep 20))))

(defn read-once
  "A generator which reads exactly once."
  []
  (gen/clients
    (gen/once {:type :invoke, :f :read})))

(defn es-test
  "Defaults for testing elasticsearch."
  [name opts]
  (merge tests/noop-test
         {:name (str "elasticsearch " name)
          :os   debian/os
          :db   (db "1.5.0")}
         opts))

(defn create-test
  "A generic create test."
  [name opts]
  (es-test (str "create " name)
           (merge {:client  (create-set-client)
                   :model   (model/set)
                   :checker (checker/compose
                              {:html    timeline/html
                               :set     checker/set
                               :latency (checker/latency-graph
                                          "report")})}
                  opts)))

(defn create-isolate-primaries-test
  "Inserts docs into a set while isolating all primaries with a partition."
  []
  (create-test "isolate primaries"
               {:nemesis   isolate-self-primaries-nemesis
                :generator (gen/phases
                             (->> (adds)
                                  (gen/stagger 1/10)
                                  (gen/delay 1)
                                  (gen/nemesis
                                    (gen/seq (cycle
                                               [(gen/sleep 30)
                                                {:type :info :f :start}
                                                (gen/sleep 200)
                                                {:type :info :f :stop}])))
                                  (gen/time-limit 800))
                             (recover)
                             (read-once))}))

(defn create-pause-test
  "Inserts docs into a set while pausing random primaries with SIGSTOP/SIGCONT."
  []
  (create-test "pause"
               {:nemesis   (nemesis/hammer-time
                             (comp rand-nth self-primaries) "java")
                :generator (gen/phases
                             (->> (adds)
                                  (gen/stagger 1/10)
                                  (gen/delay 1)
                                  (gen/nemesis
                                    (gen/seq (cycle
                                               [(gen/sleep 10)
                                                {:type :info :f :start}
                                                (gen/sleep 120)
                                                {:type :info :f :stop}])))
                                  (gen/time-limit 600))
                             (recover)
                             (read-once))}))

(defn create-crash-test
  "Inserts docs into a set while killing random nodes and restarting them."
  []
  (create-test "crash"
               {:nemesis   crash-nemesis
                :generator (gen/phases
                             (->> (adds)
                                  (gen/stagger 1/10)
                                  (gen/delay 1/10)
                                  (gen/nemesis
                                    (gen/seq (cycle
                                               [(gen/sleep 1)
                                                {:type :info :f :start}
                                                (gen/sleep 1)
                                                {:type :info :f :stop}])))
                                  (gen/time-limit 600))
                             (recover)
                             (read-once))}))

(defn create-bridge-test
  "Inserts docs into a set while weaving the network into happy little
  intersecting majority rings"
  []
  (create-test "bridge"
               {:nemesis   (nemesis/partitioner (comp nemesis/bridge shuffle))
                :generator (gen/phases
                             (->> (adds)
                                  (gen/stagger 1/10)
                                  (gen/delay 1)
                                  (gen/nemesis
                                    (gen/seq (cycle
                                               [(gen/sleep 10)
                                                {:type :info, :f :start}
                                                (gen/sleep 120)
                                                {:type :info, :f :stop}])))
                                  (gen/time-limit 600))
                             (recover)
                             (read-once))}))
