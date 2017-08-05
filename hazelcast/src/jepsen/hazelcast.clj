(ns jepsen.hazelcast
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [core :as jepsen]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian])
  (:import (com.hazelcast.client.config ClientConfig)
           (com.hazelcast.client HazelcastClient)
           (com.hazelcast.core HazelcastInstance)
           (com.hazelcast.core IMap)
           (com.hazelcast.spi.discovery NodeFilter)))

(def local-server-dir
  "Relative path to local server project directory"
  "server")

(def local-server-jar
  "Relative to server fat jar"
  (str local-server-dir
       "/target/jepsen.hazelcast-server-0.1.0-SNAPSHOT-standalone.jar"))

(def dir
  "Remote path for hazelcast stuff"
  "/opt/hazelcast")

(def jar
  "Full path to Hazelcast server jar on remote nodes."
  (str dir "/server.jar"))

(def pid-file (str dir "/server.pid"))
(def log-file (str dir "/server.log"))

(defn build-server!
  "Ensures the server jar is ready"
  [test node]
  (when (= node (jepsen/primary test))
    (when-not (.exists (io/file local-server-jar))
      (info "Building server")
      (let [{:keys [exit out err]} (sh "lein" "uberjar" :dir "server")]
        (info out)
        (info err)
        (info exit)
        (assert (zero? exit))))))

(defn install!
  "Installs the server on remote nodes."
  []
  (c/cd dir
    (c/exec :mkdir :-p dir)
    (c/upload (.getCanonicalPath (io/file local-server-jar))
              jar)))

(defn start!
  "Launch hazelcast server"
  [test node]
  (c/cd dir
        (cu/start-daemon!
          {:chdir dir
           :logfile log-file
           :pidfile pid-file}
          "/usr/bin/java"
          :-jar jar
          :--members (->> (:nodes test)
                          (remove #{node})
                          (map cn/ip)
                          (str/join ",")))))

(defn stop!
  "Kill hazelcast server"
  [test node]
  (c/cd dir
        (c/su
          (cu/stop-daemon! pid-file))))

(defn db
  "Installs and runs hazelcast nodes"
  []
  (reify db/DB
    (setup! [_ test node]
      (build-server! test node)
      (jepsen/synchronize test)
      (debian/install-jdk8!)
      (install!)
      (start! test node)
      (Thread/sleep 15000))

    (teardown! [_ test node]
      (stop! test node)
      (c/su
        (c/exec :rm :-rf log-file pid-file)))

    db/LogFiles
    (log-files [_ test node]
      [log-file])))

(defn ^HazelcastInstance connect
  "Creates a hazelcast client for the given node."
  [node]
  (let [config (ClientConfig.)
        ; Global op timeouts
        _ (.setProperty config "hazelcast.client.heartbeat.interval" "1000")
        _ (.setProperty config "hazelcast.client.heartbeat.timeout" "5000")
        _ (.setProperty config "hazelcast.client.invocation.timeout.seconds" "5")
        _ (.setProperty config "hazelcast.heartbeat.interval.seconds" "1")
        _ (.setProperty config "hazelcast.master.confirmation.interval.seconds" "1")
        _ (.setProperty config "hazelcast.max.no.heartbeat.seconds" "5")
        _ (.setProperty config "hazelcast.max.no.master.confirmation.seconds" "10")
        _ (.setProperty config "hazelcast.operation.call.timeout.millis" "5000")

        net    (.getNetworkConfig config)
        ; Don't retry operations when network fails (!?)
        _      (.setRedoOperation net false)
        ; Timeouts
        _      (.setConnectionTimeout net 5000)
        ; Initial connection limits
        ; _      (.setConnectionAttemptPeriod 1000)
        ; _      (.setConnectionAttemptLimit 5)
        ; Don't use a local cache of the partition map
        _      (.setSmartRouting net false)
        _      (info :net net)
        ; Only talk to our node (the client's smart and will try to talk to
        ; everyone, but we're trying to simulate clients in different network
        ; components here)
        node-filter (reify NodeFilter
                      (test [this candidate]
                        (prn node :testing candidate)
                        (info node :testing candidate)
                        true))
        _      (.. net
                   getDiscoveryConfig
                   (setNodeFilter node-filter))
        ; Connect to our node
        _      (.addAddress net (into-array String [node]))]
    (HazelcastClient/newHazelcastClient config)))

(defn atomic-long-id-client
  "Generates unique IDs using an AtomicLong"
  [conn atomic-long]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (connect node)]
        (atomic-long-id-client conn
                               (.getAtomicLong conn "jepsen"))))

    (invoke! [this test op]
      (assert (= (:f op) :generate))
      (assoc op :type :ok, :value (.incrementAndGet atomic-long)))

    (teardown! [this test]
      (.terminate conn))))

(defn hazelcast-test
  "Constructs a Jepsen test map from CLI options"
  [opts]
  (merge tests/noop-test
         opts
         {:name   "hazelcast"
          :os     debian/os
          :db     (db)
          :client (atomic-long-id-client nil nil)
          :nemesis (nemesis/partition-majorities-ring)
          :generator (->> {:type :invoke, :f :generate}
                          (gen/stagger 1)
                          (gen/nemesis (gen/start-stop 5 15))
                          (gen/time-limit 120))
          :checker (checker/compose
                     {:perf     (checker/perf)
                      :timeline (timeline/html)
                      :id       (checker/unique-ids)})}))

(defn -main
  "Command line runner"
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn hazelcast-test})
                   (cli/serve-cmd))
            args))
