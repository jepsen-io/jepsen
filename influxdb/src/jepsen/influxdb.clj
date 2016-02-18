(ns jepsen.influxdb

  "Tests for InfluxDB"
  (:require [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [knossos.op :as op]
            [knossos.model :as model]
            [jepsen [client :as client]
             [core :as jepsen]
             [db :as db]
             [tests :as tests]
             [control :as c :refer [|]]
             [checker :as checker]
             [nemesis :as nemesis]
             [generator :as gen]
             [util :refer [timeout meh]]]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]
            )
  (:import (org.influxdb InfluxDBFactory InfluxDB$LogLevel))
  (:import (org.influxdb.dto Point Query BatchPoints))
  (:import (TimeUnit)
           (java.util.concurrent TimeUnit))
  )

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (double (rand-int 5))})
(defn wMulti [_ _ ] {:type :invoke, :f :write, :value (double (rand-int 5)), :time (long (rand-int ))})

(defn consToRef [element theRef]
  (dosync
    (let [newSeq (cons element (deref theRef))]
      (ref-set theRef newSeq)
      newSeq
      )
    )
  )

(defn nodesToJoinParameterString [nodes]
  (if (>= 1 (count nodes)) ""
                           (str "-join " (name (last nodes)) ":8091"))
  )

(def dbName "jepsen")

(defn connect [node]
  (let [
        c (InfluxDBFactory/connect (str "http://" (name node) ":8086") "root" "root")
        ]
    (.enableBatch c 1 1 TimeUnit/MILLISECONDS)
    (.setLogLevel c InfluxDB$LogLevel/BASIC)
    c
    )
  )

(defn batchPoints [writeConsistencyLevel]
  (->
    (BatchPoints/database dbName)
    (.consistency writeConsistencyLevel)
    (.build)
    )
  )

(defn writeToInflux [conn value writeConsistencyLevel]
  (-> conn
      (.write
        (-> (batchPoints writeConsistencyLevel)
            (.point
              (-> (Point/measurement "answers")
                  (.time 1 TimeUnit/NANOSECONDS)
                  (.field "answer" value)
                  (.build)
                )
              )
          )
        )
    )

  )

(defn queryInflux [conn queryString]
  (let [q (Query. queryString dbName)]
    (-> conn
        (.query q)
        (.getResults)
        (.get 0)
        (.getSeries)
        (.get 0)
        (.getValues)
        (.get 0)
        (.get 1)
        )
    )
  )
(defn influxClient
  "A client for a single compare-and-set register"
  [conn writeConsistencyLevel]
  (reify client/Client
    (setup! [_ test node]
      (info node "setting up client")
      (let [cl (influxClient (connect node) writeConsistencyLevel)]
        (info node "client setup!")
        cl)
      )

    (invoke! [this test op]
      (timeout 5000 (assoc op :type :info, :error :timeout)
               (case (:f op)
                 :read (assoc op :type :ok, :value
                                 (queryInflux conn "SELECT answer from answers where time = 1")
                                 )
                 :write (try
                          (do (writeToInflux conn (:value op) writeConsistencyLevel)
                              (assoc op :type :ok))
                          (catch Throwable e
                            (do
                              (info "Write failed with " e)
                              (if (.contains (.getMessage e) "timeout")
                                (assoc op :type :info, :error (.getMessage e))
                                (assoc op :type :fail, :error (.getMessage e))
                                )
                              )
                            )
                          )
                 )

               )
      )
    (teardown! [_ test])))
(defn nuke [node]
  (try
    (c/su
      (info node "Stopping influxdb...")
      (meh (c/exec :killall :-9 "influxd"))
      (c/exec :service :influxdb :stop)
      (info node "Removing influxdb...")
      (c/exec :dpkg :--purge "influxdb")
      (c/exec :rm :-rf "/var/lib/influxdb")
      (info node "Removed influxdb")
      (c/cd "/var/log/influxdb"
            (try (c/exec :gzip :-S (str (java.lang.System/currentTimeMillis)) "influxd.log")
                 (catch Exception _)
                 )
            )
      )
    (catch Throwable _)
    )
  )
(defn setupInflux [test node version nodeOrderRef]

  (info node "Starting influxdb setup.")
  (try
    (c/cd "/tmp"
          (let [file (str "influxdb_" version "_amd64.deb")]
            (when-not (cu/file? file)
              (info "Fetching deb package from influxdb repo")
              (c/exec :wget (str "https://s3.amazonaws.com/influxdb/" file)))
            (c/su
              ; Install package
              (try (c/exec :dpkg-query :-l :influxdb)
                   (catch RuntimeException _
                     (info "Installing influxdb...")
                     (c/exec :dpkg :-i file))))
            )
          (c/su
            (c/exec :cp "/usr/lib/influxdb/scripts/init.sh" "/etc/init.d/influxdb")

            (info node "Copying influxdb configuration...")
            (c/exec :echo (-> (io/resource "influxdb.conf")
                              slurp
                              (str/replace #"%HOSTNAME%" (name node)))
                    :> "/etc/influxdb/influxdb.conf")


            (c/exec :echo
                    (-> (io/resource "servers.sh") slurp) :> "/root/servers.sh")
            (c/exec :echo
                    (-> (io/resource "test_cluster.sh") slurp) :> "/root/test_cluster.sh")
            (c/exec :echo
                    (-> (io/resource "test_influx_up.sh") slurp) :> "/root/test_influx_up.sh")


            (try (c/exec :service :influxdb :stop)
                 (catch Exception _
                   (info node "no need to stop")
                   )
                 )

            )

          (info node "I am waiting for the lock...")
          (locking nodeOrderRef
            (info node "I have the lock!")
            (let [nodeOrder (consToRef node nodeOrderRef)]

              (info node "nodes to join: " (nodesToJoinParameterString nodeOrder) nodeOrder (deref nodeOrderRef))
              (let [joinParams (-> (io/resource "influxdb") slurp
                                   (str/replace #"%NODES_TO_JOIN%" (nodesToJoinParameterString nodeOrder)))]
                (info node "joinParams: " joinParams)
                (c/su
                  (c/exec :echo
                          joinParams
                          :>
                          "/etc/default/influxdb"
                          )
                  )
                )
              ; Ensure node is running
              (try (c/exec :service :influxdb :status)
                   (catch RuntimeException _
                     (info node "Starting influxdb...")
                     (c/exec :service :influxdb :start)))

              (info node "InfluxDB started!")
              )
            )

          (while
            (try (c/exec :bash "/root/test_cluster.sh")
                 false
                 (catch Exception _
                   true
                   )
                 )
            (do
              (info node "waiting for influx cluster members to join up...")
              (Thread/sleep 1000)
              )
            )
          (jepsen/synchronize test)
          (c/exec :bash "/root/test_cluster.sh")
          (let [
                c (connect node)
                initialPoint (.build (.field (.time (Point/measurement "answers") 1 TimeUnit/NANOSECONDS) "answer" 42))
                ]
            (info node "creating jepsen DB...")
            (.createDatabase c dbName)
            (.write c dbName "default" initialPoint)
            )

          (info node "This node is OK, sees 3 members in the raft cluster")
          )
    (catch RuntimeException e
      (error node "Error at Setup: " e (.getMessage e))
      (throw e)
      )
    )
  )
(defn db
  "Sets up and tears down InfluxDB"
  [version]
  (let [nodeOrderRef (ref [])]
    (reify db/DB

      (setup! [_ test node]
        (nuke node)
        (setupInflux test node version nodeOrderRef)
        )

      (teardown! [_ test node]
        ;; nothing for now for examination of running nodes
      )

      db/LogFiles
      (log-files [_ test node] ["/var/log/influxdb/influxd.log"]))
    )
  )


;(defn test-multi-shard-data
;  "Datapoints on multiple shards (from predefined weeks) are being inserted randomly and read back.
;  This should show write unavailability even in ConsistencyLevel.ANY mode"
;
;
;  )


(defn test-
  [version name writeConsistencyLevel healthy]
  (merge tests/noop-test
         {:ssh
                       {:username         "root"
                        :private-key-path "~/.ssh/id_rsa"
                        }
          :nodes       [:n1 :n2 :n3]
          :name        name
          :concurrency 3
          :os          debian/os
          :client      (influxClient nil writeConsistencyLevel)
          :db          (db version)
          :nemesis     (if healthy nemesis/noop (nemesis/partition-halves))
          :generator   (->> (gen/mix [r w])
                            (gen/stagger 1)
                            (gen/nemesis
                              (gen/seq
                                (cycle [(gen/sleep 1)
                                        {:type :info :f :start}
                                        (gen/sleep 3)
                                        {:type :info :f :stop}
                                        ])
                                )
                              )
                            (gen/time-limit 30))
          :model       (model/register 42.0)
          :checker     (checker/compose
                         {:perf   (checker/perf)
                          :linear checker/linearizable})
          }
         )

  )


(defn test-on-single-shard-data
  "Testing datapoints inserted to the very same time point, i.e. emulating a simple read-write register.
  This should demonstrate InfluxDB's AP behaviour with ConsistencyLevel.ANY and
  write unavailability with ConsistencyLevel.ALL in case of network partitions"
  [version name writeConsistencyLevel healthy]
  (test- version (str name " writeConsistency: " writeConsistencyLevel) writeConsistencyLevel healthy)
  )

