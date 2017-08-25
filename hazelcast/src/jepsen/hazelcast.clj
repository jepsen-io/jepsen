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
  (:import (java.util.concurrent TimeUnit)
           (com.hazelcast.client.config ClientConfig)
           (com.hazelcast.client HazelcastClient)
           (com.hazelcast.core HazelcastInstance)
           (com.hazelcast.core IMap)
           (com.hazelcast.spi.discovery NodeFilter)))

(def local-server-dir
  "Relative path to local server project directory"
  "server")

(def local-server-jar
  "Relative to server fat jar"
  (str local-server-dir "/target/hazelcast-server.jar"))

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

        net    (doto (.getNetworkConfig config)
                 ; Don't retry operations when network fails (!?)
                 (.setRedoOperation false)
                 ; Timeouts
                 (.setConnectionTimeout 5000)
                 ; Initial connection limits
                 (.setConnectionAttemptPeriod 1000)
                 ; Try reconnecting indefinitely
                 (.setConnectionAttemptLimit 0)
                 ; Don't use a local cache of the partition map
                 (.setSmartRouting false))
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
                               (.getAtomicLong conn "jepsen.atomic-long"))))

    (invoke! [this test op]
      (assert (= (:f op) :generate))
      (assoc op :type :ok, :value (.incrementAndGet atomic-long)))

    (teardown! [this test]
      (.terminate conn))))

(defn id-gen-id-client
  "Generates unique IDs using an IdGenerator"
  [conn id-gen]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (connect node)]
        (id-gen-id-client conn
                          (.getIdGenerator conn "jepsen.id-gen"))))

    (invoke! [this test op]
      (assert (= (:f op) :generate))
      (assoc op :type :ok, :value (.newId id-gen)))

    (teardown! [this test]
      (.terminate conn))))

(def queue-poll-timeout
  "How long to wait for items to become available in the queue, in ms"
  1)

(defn queue-client
  "Uses :enqueue, :dequeue, and :drain events to interact with a Hazelcast
  queue."
  ([]
   (queue-client nil nil))
  ([conn queue]
   (reify client/Client
     (setup! [_ test node]
       (let [conn (connect node)]
         (queue-client conn (.getQueue conn "jepsen.queue"))))

     (invoke! [this test op]
       (case (:f op)
         :enqueue (do (.put queue (:value op))
                      (assoc op :type :ok))
         :dequeue (if-let [v (.poll queue
                                 queue-poll-timeout TimeUnit/MILLISECONDS)]
                    (assoc op :type :ok, :value v)
                    (assoc op :type :fail, :error :empty))
         :drain   (loop [values []]
                    (if-let [v (.poll queue
                                      queue-poll-timeout TimeUnit/MILLISECONDS)]
                      (recur (conj values v))
                      (assoc op :type :ok, :value values)))))

     (teardown! [this test]
       (.terminate conn)))))

(defn queue-gen
  "A generator for queue operations. Emits enqueues of sequential integers."
  []
  (let [next-element (atom -1)]
    (->> (gen/mix [(fn enqueue-gen [_ _]
                     {:type  :invoke
                      :f     :enqueue
                      :value (swap! next-element inc)})
                   {:type :invoke, :f :dequeue}])
         (gen/stagger 1))))

(defn queue-client-and-gens
  "Constructs a queue client and generator. Returns {:client
  ..., :generator ...}."
  []
  {:client          (queue-client)
   :generator       (queue-gen)
   :final-generator (->> {:type :invoke, :f :drain}
                         gen/once
                         gen/each)})

(defn lock-client
  ([] (lock-client nil nil))
  ([conn lock]
   (reify client/Client
     (setup! [_ test node]
       (let [conn (connect node)]
         (lock-client conn (.getLock conn "jepsen.lock"))))

     (invoke! [this test op]
       (try
         (case (:f op)
           :acquire (if (.tryLock lock 5000 TimeUnit/MILLISECONDS)
                      (assoc op :type :ok)
                      (assoc op :type :fail))
           :release (do (.unlock lock)
                        (assoc op :type :ok)))
        (catch com.hazelcast.quorum.QuorumException e
          (Thread/sleep 1000)
          (assoc op :type :fail, :error :quorum))
        (catch java.lang.IllegalMonitorStateException e
          (Thread/sleep 1000)
          (if (re-find #"Current thread is not owner of the lock!"
                       (.getMessage e))
            (assoc op :type :fail, :error :not-lock-owner)
            (throw e)))
        (catch java.io.IOException e
          (Thread/sleep 1000)
          (condp re-find (.getMessage e)
            ; This indicates that the Hazelcast client doesn't have a remote
            ; peer available, and that the message was never sent.
            #"Packet is not send to owner address"
            (assoc op :type :fail, :error :client-down)

            (throw e)))))

     (teardown! [this test]
       (.terminate conn)))))

(defn crdt-map-client
  ([] (crdt-map-client nil nil))
  ([conn m]
   (reify client/Client
     (setup! [_ test node]
       (let [conn (connect node)]
         (crdt-map-client conn (.getMap conn "jepsen.map"))))

     (invoke! [this test op]
       (case (:f op)
         ; Note that Hazelcast serialization doesn't seem to know how to
         ; replace types like HashSet, so we're storing our sets as sorted long
         ; arrays instead.
         :add (if-let [v (.get m "hi")]
                ; We have a current set.
                (let [s  (into (sorted-set) v)
                      s' (conj s (:value op))
                      v' (long-array s')]
                  (if (.replace m "hi" v v')
                    (assoc op :type :ok)
                    (assoc op :type :fail, :error :cas-failed)))

                ; We're starting fresh.
                (let [v' (long-array (sorted-set (:value op)))]
                  ; Note that replace and putIfAbsent have opposite senses for
                  ; their return values.
                  (if (.putIfAbsent m "hi" v')
                    (assoc op :type :fail, :error :cas-failed)
                    (assoc op :type :ok))))

         :read (assoc op :type :ok, :value (into (sorted-set) (.get m "hi")))))

     (teardown! [this test]
       (.terminate conn)))))

(defn workloads
  "The workloads we can run. Each workload is a map like

      {:generator         a generator of client ops
       :final-generator   a generator to run after the cluster recovers
       :client            a client to execute those ops
       :checker           a checker
       :model             for the checker}

  Note that workloads are *stateful*, since they include generators; that's why
  this is a function, instead of a constant--we may need a fresh workload if we
  run more than one test."
  []
  {:crdt-map         {:client    (crdt-map-client)
                      :generator (->> (range)
                                      (map (fn [x] {:type  :invoke
                                                    :f     :add
                                                    :value x}))
                                      gen/seq
                                      (gen/stagger 1))
                      :final-generator (->> {:type :invoke, :f :read}
                                            gen/once
                                            gen/each)
                      :checker   (checker/set)}
   :lock             {:client    (lock-client)
                      :generator (->> [{:type :invoke, :f :acquire}
                                       {:type :invoke, :f :release}]
                                      cycle
                                      gen/seq
                                      gen/each)
                      :checker   (checker/linearizable)
                      :model     (model/mutex)}
   :queue            (assoc (queue-client-and-gens)
                            :checker (checker/total-queue))
   :atomic-long-ids  {:client (atomic-long-id-client nil nil)
                      :generator (->> {:type :invoke, :f :generate}
                                      (gen/stagger 1))
                      :checker  (checker/unique-ids)}
   :id-gen-ids       {:client    (id-gen-id-client nil nil)
                      :generator {:type :invoke, :f :generate}
                      :checker   (checker/unique-ids)}})

(defn hazelcast-test
  "Constructs a Jepsen test map from CLI options"
  [opts]
  (let [{:keys [generator
                final-generator
                client
                checker
                model]} (get (workloads) (:workload opts))
        generator (->> generator
                       (gen/nemesis (gen/start-stop 30 15))
                       (gen/time-limit (:time-limit opts)))
        generator (if-not final-generator
                    generator
                    (gen/phases generator
                                (gen/log "Healing cluster")
                                (gen/nemesis
                                  (gen/once {:type :info, :f :stop}))
                                (gen/log "Waiting for quiescence")
                                (gen/sleep 500)
                                (gen/clients final-generator)))]
    (merge tests/noop-test
           opts
           {:name       (str "hazelcast " (name (:workload opts)))
            :os         debian/os
            :db         (db)
            :client     client
            :nemesis    (nemesis/partition-majorities-ring)
            :generator  generator
            :checker    (checker/compose
                          {:perf     (checker/perf)
                           :timeline (timeline/html)
                           :workload checker})
            :model      model})))

(def opt-spec
  "Additional command line options"
  [[nil "--workload WORKLOAD" "Test workload to run, e.g. atomic-long-ids."
    :parse-fn keyword
    :missing  (str "--workload " (cli/one-of (workloads)))
    :validate [(workloads) (cli/one-of (workloads))]]])

(defn -main
  "Command line runner."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn   hazelcast-test
                                         :opt-spec  opt-spec})
                   (cli/serve-cmd))
            args))
