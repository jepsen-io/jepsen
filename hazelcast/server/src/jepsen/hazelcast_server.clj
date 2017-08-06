(ns jepsen.hazelcast-server
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (com.hazelcast.core Hazelcast)
           (com.hazelcast.config Config
                                 LockConfig
                                 MapConfig
                                 QuorumConfig)))

(def opt-spec
  [["-m" "--members MEMBER-LIST" "Comma-separated list of peers to connect to"
    :parse-fn (fn [m]
                (str/split m #"\s*,\s*"))]])

(defn -main
  "Go go go"
  [& args]
  (let [{:keys [options
                arguments
                summary
                errors]} (cli/parse-opts args opt-spec)
        config  (Config.)

        ; Timeouts
        _ (.setProperty config "hazelcast.client.heartbeat.interval" "1000")
        _ (.setProperty config "hazelcast.client.heartbeat.timeout" "5000")
        _ (.setProperty config "hazelcast.client.invocation.timeout.seconds" "5")
        _ (.setProperty config "hazelcast.heartbeat.interval.seconds" "1")
        _ (.setProperty config "hazelcast.master.confirmation.interval.seconds" "1")
        _ (.setProperty config "hazelcast.max.no.heartbeat.seconds" "5")
        _ (.setProperty config "hazelcast.max.no.master.confirmation.seconds" "10")
        _ (.setProperty config "hazelcast.operation.call.timeout.millis" "5000")

        ; Network config
        _       (.. config getNetworkConfig getJoin getMulticastConfig
                    (setEnabled false))
        tcp-ip  (.. config getNetworkConfig getJoin getTcpIpConfig)
        _       (doseq [member (:members options)]
                  (.addMember tcp-ip member))
        _       (.setEnabled tcp-ip true)

        ; Quorum for split-brain protection
        quorum (doto (QuorumConfig.)
                 (.setName "majority")
                 (.setEnabled true)
                 (.setSize (inc (int (Math/floor
                                       (/ (inc (count (:members options)))
                                          2))))))
        _ (.addQuorumConfig config quorum)


        ; Locks
        lock-config (doto (LockConfig.)
                      (.setName "jepsen.lock")
                      (.setQuorumName "majority"))
        _ (.addLockConfig config lock-config)

        ; Queues
        queue-config (doto (.getQueueConfig config "jepsen.queue")
                       (.setName "jepsen.queue")
                       (.setBackupCount 2)
                       (.setQuorumName "majority"))
        _ (.addQueueConfig config queue-config)


        ; Maps
        map-config (doto (MapConfig.)
                    (.setName "jepsen.map")
                    (.setQuorumName "majority"))
        _ (.addMapConfig config map-config)

        ; Launch
        hc      (Hazelcast/newHazelcastInstance config)]
    (loop []
      (Thread/sleep 1000))))
