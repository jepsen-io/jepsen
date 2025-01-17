(ns jepsen.hazelcast-server
  (:gen-class)
  (:require [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import (com.hazelcast.core Hazelcast)
           (com.hazelcast.config.cp FencedLockConfig CPSemaphoreConfig)
           (com.hazelcast.config Config
                                 LockConfig
                                 MapConfig
                                 QuorumConfig)
           ))

(def opt-spec
  [["-m" "--members MEMBER-LIST" "Comma-separated list of peers to connect to"
    :parse-fn (fn [m]
                  (str/split m #"\s*,\s*"))]])

(defn prepare-cp-subsystem-config
  "Prepare Hazelcast CPSubsystemConfig"
  [config members]
  (let [cpSubsystemConfig (.getCPSubsystemConfig config)
        raftAlgorithmConfig (.getRaftAlgorithmConfig cpSubsystemConfig)
        semaphoreConfig (CPSemaphoreConfig. "jepsen.cpSemaphore" false)
        lockConfig1 (FencedLockConfig. "jepsen.cpLock1" 1)
        lockConfig2 (FencedLockConfig. "jepsen.cpLock2" 2)]

       (.setLeaderElectionTimeoutInMillis raftAlgorithmConfig 1000)
       (.setLeaderHeartbeatPeriodInMillis raftAlgorithmConfig 1500)
       (.setCommitIndexAdvanceCountToSnapshot raftAlgorithmConfig 250)
       (.setFailOnIndeterminateOperationState cpSubsystemConfig true)

       (.setCPMemberCount cpSubsystemConfig (count members))
       (.setSessionHeartbeatIntervalSeconds cpSubsystemConfig 5)
       (.setSessionTimeToLiveSeconds cpSubsystemConfig 300)

       (.addSemaphoreConfig cpSubsystemConfig semaphoreConfig)
       (.addLockConfig cpSubsystemConfig lockConfig1)
       (.addLockConfig cpSubsystemConfig lockConfig2)
       cpSubsystemConfig))

(defn -main
  "Go go go"
  [& args]
  (let [{:keys [options
                arguments
                summary
                errors]} (cli/parse-opts args opt-spec)
        config  (Config.)
        members (:members options)

        ; Timeouts
        _ (.setProperty config "hazelcast.client.max.no.heartbeat.seconds" "90")
        _ (.setProperty config "hazelcast.heartbeat.interval.seconds" "1")
        _ (.setProperty config "hazelcast.max.no.heartbeat.seconds" "5")
        _ (.setProperty config "hazelcast.operation.call.timeout.millis" "5000")
        _ (.setProperty config "hazelcast.wait.seconds.before.join" "0")
        _ (.setProperty config "hazelcast.merge.first.run.delay.seconds" "1")
        _ (.setProperty config "hazelcast.merge.next.run.delay.seconds" "1")

        ; Network config
        _       (.. config getNetworkConfig getJoin getMulticastConfig
                    (setEnabled false))
        tcp-ip  (.. config getNetworkConfig getJoin getTcpIpConfig)
        _       (doseq [member members]
                  (.addMember tcp-ip member))
        _       (.setEnabled tcp-ip true)

        ; prepare the CP subsystem
        _ (prepare-cp-subsystem-config config members)

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

        ; Maps with CRDTs
        crdt-map-config (doto (MapConfig.)
                    (.setName "jepsen.crdt-map")
                    (.setMergePolicy
                      "jepsen.hazelcast_server.SetUnionMergePolicy"))
        _ (.addMapConfig config crdt-map-config)

        ; Maps without CRDTs
        map-config (doto (MapConfig.)
                     (.setName "jepsen.map")
                     (.setQuorumName "majority"))
        _ (.addMapConfig config map-config)

        ; Launch
        hc      (Hazelcast/newHazelcastInstance config)]
    (loop []
      (Thread/sleep 1000))))
