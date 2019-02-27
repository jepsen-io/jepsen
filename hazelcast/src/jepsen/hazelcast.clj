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
             [reconnect :as rc]
             [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian])
  (:import (java.util.concurrent TimeUnit)
           (com.hazelcast.client.config ClientConfig)
           (com.hazelcast.client HazelcastClient)
           (com.hazelcast.core HazelcastInstance)
           (knossos.model Model)
           (java.util UUID)
           (java.io IOException)
           (com.hazelcast.quorum QuorumException)))

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

(def CLIENT_ID_SEPARATOR "xxxxxxxxxx")
(def REENTRANT_LOCK_ACQUIRE_COUNT 2)
(def NUMBER_OF_PERMITS 2)
(def LOGGING_SLEEP_DURATION_MS 1000)
(def INVALID_FENCE 0)

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
      (assert (zero? exit)))))
 )

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
          {:chdir   dir
           :logfile log-file
           :pidfile pid-file}
          "/usr/bin/java"
          :-jar jar
          :--members (->> (:nodes test)
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
        _ (.setInstanceName config node)

        net (doto (.getNetworkConfig config)
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
        _ (info :net net)
        ; Only talk to our node (the client's smart and will try to talk to
        ; everyone, but we're trying to simulate clients in different network
        ; components here)
        ; Connect to our node
        _ (.addAddress net (into-array String [node]))]
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
      (.shutdown conn))))


(defn create-cp-atomic-long
  "Creates a new CP based AtomicLong"
  [client name]
  (.getAtomicLong (.getCPSubsystem client) name))


(defn create-cp-atomic-reference
  "Creates a new CP based AtomicReference"
  [client name]
  (.getAtomicReference (.getCPSubsystem client) name))


(defn cp-atomic-long-id-client
  "Generates unique IDs using a CP AtomicLong"
  [conn atomic-long]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (connect node)]
        (cp-atomic-long-id-client conn (create-cp-atomic-long conn "jepsen.atomic-long"))))

    (invoke! [this test op]
      (assert (= (:f op) :generate))
      (assoc op :type :ok, :value (.incrementAndGet atomic-long)))

    (teardown! [this test]
      (.shutdown conn))))

(defn cp-cas-long-client
  "A CAS register using a CP AtomicLong"
  [conn atomic-long]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (connect node)]
        (cp-cas-long-client conn (create-cp-atomic-long conn "jepsen.cas-long"))))

    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (.get atomic-long))
        :write (do (.set atomic-long (:value op))
                   (assoc op :type :ok))
        :cas (let [[currentV newV] (:value op)]
               (if (.compareAndSet atomic-long currentV newV)
                 (assoc op :type :ok)
                 (assoc op :type :fail :error :cas-failed)
                 ))
        )
      )

    (teardown! [this test]
      (.shutdown conn))))

(defn cp-cas-reference-client
  "A CAS register using a CP AtomicReference"
  [conn atomic-ref]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (connect node)]
        (cp-cas-reference-client conn (create-cp-atomic-reference conn "jepsen.cas-register"))))

    (invoke! [this test op]
      (case (:f op)
        :read (assoc op :type :ok, :value (.get atomic-ref))
        :write (do (.set atomic-ref (:value op))
                   (assoc op :type :ok))
        :cas (let [[currentV newV] (:value op)]
               (if (.compareAndSet atomic-ref currentV newV)
                 (assoc op :type :ok)
                 (assoc op :type :fail :error :cas-failed)
                 ))
        )
      )

    (teardown! [this test]
      (.shutdown conn))))

(defn atomic-ref-id-client
  "Generates unique IDs using an AtomicReference"
  [conn atomic-ref]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (connect node)]
        (atomic-ref-id-client conn
                              (.getAtomicReference conn "jepsen.atomic-ref"))))

    (invoke! [this test op]
      (assert (= (:f op) :generate))
      (let [v (.get atomic-ref)
            v' (inc (or v 0))]
        (if (.compareAndSet atomic-ref v v')
          (assoc op :type :ok, :value v')
          (assoc op :type :fail, :error :cas-failed))))

    (teardown! [this test]
      (.shutdown conn))))

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
      (.shutdown conn))))

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
         :drain (loop [values []]
                  (if-let [v (.poll queue
                                    queue-poll-timeout TimeUnit/MILLISECONDS)]
                    (recur (conj values v))
                    (assoc op :type :ok, :value values)))))

     (teardown! [this test]
       (.shutdown conn)))))

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

(defn log-acquire-ok
  [clientName op]
  (do
    (try (Thread/sleep LOGGING_SLEEP_DURATION_MS) (catch Exception _))
    (info (str "_" clientName "_acquire_ok " (:value op)))))

(defn log-acquire-fail
  [clientName op]
  (do
    (try (Thread/sleep LOGGING_SLEEP_DURATION_MS) (catch Exception _))
    (info (str "_" clientName "_acquire_fail " (:value op)))))

(defn log-acquire-maybe
  [clientName op exception]
  (do
    (try (Thread/sleep LOGGING_SLEEP_DURATION_MS) (catch Exception _))
    (info (str "_" clientName "_acquire_maybe " (:value op) " exception: " (.getMessage exception)))))

(defn log-release-ok
  [clientName op]
  (do
    (try (Thread/sleep LOGGING_SLEEP_DURATION_MS) (catch Exception _))
    (info (str "_" clientName "_release_ok " (:value op)))))

(defn log-release-fail
  [clientName op]
  (do
    (try (Thread/sleep LOGGING_SLEEP_DURATION_MS) (catch Exception _))
    (info (str "_" clientName "_release_fail " (:value op)))))

(defn log-release-maybe
  [clientName op exception]
  (do
    (try (Thread/sleep LOGGING_SLEEP_DURATION_MS) (catch Exception _))
    (info (str "_" clientName "_release_maybe " (:value op) " exception: " (.getMessage exception)))))

(defn invoke-acquire [clientName lock op]
  (let [fence (.tryLockAndGetFence lock 5000 TimeUnit/MILLISECONDS)]
    (if (not= fence INVALID_FENCE)
      (do
        (log-acquire-ok clientName op)
        (assoc op :type :ok :value {:client clientName :fence fence :uid (:value op)})
        )
      (do
        (log-acquire-fail clientName op)
        (assoc op :type :fail)
        )
      )
    )
  )

(defn fenced-lock-client
  ([lock-name] (fenced-lock-client nil nil lock-name))
  ([conn lock lock-name]
   (reify client/Client
     (setup! [_ test node]
       (let [conn (connect node)]
         (fenced-lock-client conn (.getLock (.getCPSubsystem conn) lock-name) lock-name)
         )
       )

     (invoke! [this test op]
       (let [clientName (.getName conn)]
         (try
           (info (str " " CLIENT_ID_SEPARATOR " " clientName " " CLIENT_ID_SEPARATOR " " op))
           (case (:f op)
             :acquire (invoke-acquire clientName lock op)
             :release (do
                        (.unlock lock)
                        (log-release-ok clientName op)
                        (assoc op :type :ok :value {:client clientName :uid (:value op)})
                        )
             )
           (catch IllegalMonitorStateException e
             (case (:f op) :acquire (log-acquire-fail clientName op) :release (log-release-fail clientName op))
             (assoc op :type :fail, :error :not-lock-owner)
             )
           (catch IOException e
             (condp re-find (.getMessage e)
               ; This indicates that the Hazelcast client doesn't have a remote
               ; peer available, and that the message was never sent.
               #"Packet is not send to owner address"
               (do
                 (case (:f op) :acquire (log-acquire-fail clientName op) :release (log-release-fail clientName op))
                 (assoc op :type :fail, :error :client-down)
                 )
               (do
                 (case (:f op) :acquire (log-acquire-maybe clientName op e) :release (log-release-maybe clientName op e))
                 (assoc op :type :info, :error :io-exception)
                 )
               )
             )
           (catch Exception e
             (case (:f op) :acquire (log-acquire-maybe clientName op e) :release (log-release-maybe clientName op e))
             (assoc op :type :info, :error :exception)
             )
           )
         )
       )

     (teardown! [this test]
       (try
         (.terminate (.getLifecycleService conn))
         (catch Exception e
           (warn "exception while client termination")
           (.printStackTrace e)
           )
         )
       )

     )
    )
  )

(defn cp-semaphore-client
  ([] (cp-semaphore-client nil nil))
  ([conn semaphore]
   (reify client/Client
     (setup! [_ test node]
       (let [conn (connect node)
             sem (.getSemaphore (.getCPSubsystem conn) "jepsen.cpSemaphore")
             _ (.init sem NUMBER_OF_PERMITS)]
         (cp-semaphore-client conn sem)
         )
       )

     (invoke! [this test op]
       (let [clientName (.getName conn)]
         (try
           (info (str " " CLIENT_ID_SEPARATOR " " clientName " " CLIENT_ID_SEPARATOR " " op))
           (case (:f op)
             :acquire (if (.tryAcquire semaphore 5000 TimeUnit/MILLISECONDS)
                        (do
                          (log-acquire-ok clientName op)
                          (assoc op :type :ok :value {:client clientName :uid (:value op)})
                          )
                        (do
                          (log-acquire-fail clientName op)
                          (assoc op :type :fail :debug {:client clientName :uid (:value op)}))
                        )
             :release (do
                        (.release semaphore)
                        (log-release-ok clientName op)
                        (assoc op :type :ok :value {:client clientName :uid (:value op)})
                        )
             )
           (catch IllegalArgumentException e
             (case (:f op) :acquire (log-acquire-fail clientName op) :release (log-release-fail clientName op))
             (assoc op :type :fail :error :not-permit-owner :debug {:client clientName :uid (:value op)})
             )
           (catch IOException e
             (condp re-find (.getMessage e)
               ; This indicates that the Hazelcast client doesn't have a remote
               ; peer available, and that the message was never sent.
               #"Packet is not send to owner address"
               (do
                 (case (:f op) :acquire (log-acquire-fail clientName op) :release (log-release-fail clientName op))
                 (assoc op :type :fail :error :client-down :debug {:client clientName :uid (:value op)})
                 )
               (do
                 (case (:f op) :acquire (log-acquire-maybe clientName op e) :release (log-release-maybe clientName op e))
                 (assoc op :type :info :error :io-exception :debug {:client clientName :uid (:value op)}))
               )
             )
           (catch Exception e
             (case (:f op) :acquire (log-acquire-maybe clientName op e) :release (log-release-maybe clientName op e))
             (assoc op :type :info :error :exception :debug {:client clientName :uid (:value op)})
             )
           )
         )
       )

     (teardown! [this test]
       (try
         (.terminate (.getLifecycleService conn))
         (catch Exception e
           (warn "exception while client termination")
           (.printStackTrace e)
           )
         )
       )

     )
    )
  )

(defn lock-client
  ([lock-name] (lock-client nil nil lock-name))
  ([conn lock lock-name]
   (reify client/Client
     (setup! [_ test node]
       (let [conn (connect node)]
         (lock-client conn (.getLock conn lock-name) lock-name)))

     (invoke! [this test op]
       (try
         (case (:f op)
           :acquire (if (.tryLock lock 5000 TimeUnit/MILLISECONDS)
                      (assoc op :type :ok)
                      (assoc op :type :fail))
           :release (do (.unlock lock)
                        (assoc op :type :ok)))
         (catch QuorumException e
           (Thread/sleep 1000)
           (assoc op :type :fail, :error :quorum))
         (catch IllegalMonitorStateException e
           (Thread/sleep 1000)
           (if (re-find #"Current thread is not owner of the lock!"
                        (.getMessage e))
             (assoc op :type :fail, :error :not-lock-owner)
             (throw e)))
         (catch IOException e
           (Thread/sleep 1000)
           (condp re-find (.getMessage e)
             ; This indicates that the Hazelcast client doesn't have a remote
             ; peer available, and that the message was never sent.
             #"Packet is not send to owner address"
             (assoc op :type :fail, :error :client-down)

             (throw e)))))

     (teardown! [this test]
       (.shutdown conn)))))

(def map-name "jepsen.map")
(def crdt-map-name "jepsen.crdt-map")

(defn map-client
  "Options:
    :crdt? - If true, use CRDTs for merging divergent maps."
  ([opts] (map-client nil nil opts))
  ([conn m opts]
   (reify client/Client
     (setup! [_ test node]
       (let [conn (connect node)]
         (map-client conn
                     (.getMap conn (if (:crdt? opts)
                                     crdt-map-name
                                     map-name))
                     opts)))

     (invoke! [this test op]
       (case (:f op)
         ; Note that Hazelcast serialization doesn't seem to know how to
         ; replace types like HashSet, so we're storing our sets as sorted long
         ; arrays instead.
         :add (if-let [v (.get m "hi")]
                ; We have a current set.
                (let [s (into (sorted-set) v)
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
       (.shutdown conn)))))

(defn map-workload
  "A workload for map tests, with the given client options."
  [client-opts]
  {:client          (map-client client-opts)
   :generator       (->> (range)
                         (map (fn [x] {:type  :invoke
                                       :f     :add
                                       :value x}))
                         gen/seq
                         (gen/stagger 1/10))
   :final-generator (->> {:type :invoke, :f :read}
                         gen/once
                         gen/each)
   :checker         (checker/set)})



(defn parseLine [line]
  (let
    [tokens (.split line CLIENT_ID_SEPARATOR)]
    [(:value (clojure.edn/read-string (nth tokens 2))) (.trim (nth tokens 1))]))

(def clientUidsToClientNames (memoize (fn []
                            (apply array-map
                                   (flatten
                                     (map parseLine
                                          (.split (:out (sh "grep" CLIENT_ID_SEPARATOR "store/latest/jepsen.log")) "\n")
                                          ))))))

(defn getClient [op]
  (let [val (:value op)]
    (if (map? val) (:client val) (get (clientUidsToClientNames) (:value op)))))

(defrecord ReentrantMutex [owner lockCount]
  Model
  (step [this op]
    (let [client (getClient op)]
      (if (nil? client)
        (do
          (info "no owner!")
          (knossos.model/inconsistent "no owner!")
          )
        (condp = (:f op)
          :acquire (if (and (< lockCount REENTRANT_LOCK_ACQUIRE_COUNT) (or (nil? owner) (= owner client)))
                     (ReentrantMutex. client (+ lockCount 1))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     )
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (ReentrantMutex. (if (= lockCount 1) nil owner) (- lockCount 1))
                     )
          )
        )
      )
    )

  Object
  (toString [this] (str "owner: " owner ", lockCount: " lockCount)))

(defn createReentrantMutex []
  "A single reentrant mutex responding to :acquire and :release messages"
  (ReentrantMutex. nil 0))



(defrecord OwnerAwareMutex [owner]
  Model
  (step [this op]
    (let [client (getClient op)]
      (if (nil? client)
        (do
          (info "no owner!")
          (knossos.model/inconsistent "no owner!")
          )
        (condp = (:f op)
          :acquire (if (nil? owner)
                     (OwnerAwareMutex. client)
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     )
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (OwnerAwareMutex. nil)
                     )
          )
        )
      )
    )

  Object
  (toString [this] (str "owner: " owner)))

(defn createOwnerAwareMutex []
  "A single non-reentrant mutex responding to :acquire and :release messages and tracking mutex owner"
  (OwnerAwareMutex. nil))



(defn getFence [op]
  (let [val (:value op)]
    (if (map? val) (:fence val) INVALID_FENCE)))

(defrecord FencedMutex [owner lockFence prevOwner]
  Model
  (step [this op]
    (let [client (getClient op) fence (getFence op)]
      (if (nil? client)
        (do
          (info "no owner!")
          (knossos.model/inconsistent "no owner!")
          )
        (condp = (:f op)
          :acquire (cond
                     (some? owner) (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (= fence INVALID_FENCE) (FencedMutex. client lockFence owner)
                     (> fence lockFence) (FencedMutex. client fence owner)
                     :else (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     )
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (FencedMutex. nil lockFence owner)
                     )
          )
        )
      )
    )

  Object
  (toString [this] (str "owner: " owner " lock fence: " lockFence " prev owner: " prevOwner)))

(defn createFencedMutex []
  "A fenced mutex responding to :acquire and :release messages and tracking monotonicity of observed fences"
  (FencedMutex. nil INVALID_FENCE nil))



(defrecord ReentrantFencedMutex [owner lockCount currentFence highestObservedFence highestObservedFenceOwner]
  Model
  (step [this op]
    (let [client (getClient op) fence (getFence op)]
      (if (nil? client)
        (do
          (info "no owner!")
          (knossos.model/inconsistent "no owner!")
          )
        (condp = (:f op)
          :acquire (cond
                     ; if the lock is not held
                     (nil? owner)
                      (cond
                        ; I can have an invalid fence or a fence larger than highestObservedFence
                        (or (= fence INVALID_FENCE) (> fence highestObservedFence))
                        (ReentrantFencedMutex. client 1 fence (max fence highestObservedFence) highestObservedFenceOwner)
                        :else
                        (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                      )
                     ; if the new acquire does not match to the current lock owner, or the lock is already acquired twice, we cannot acquire anymore
                     (or (not= owner client) (= lockCount REENTRANT_LOCK_ACQUIRE_COUNT)) (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     ; if the lock is acquired without a fence, and the new acquire has no fence or a fence larger than highestObservedFence
                     (= currentFence INVALID_FENCE) (cond (or (= fence INVALID_FENCE) (> fence highestObservedFence))
                                                      (ReentrantFencedMutex. client 2 fence (max fence highestObservedFence) highestObservedFenceOwner)
                                                    :else
                                                      (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
                     ; if the lock is acquired with a fence, and the new acquire has no fence or the same fence
                     (or (= fence INVALID_FENCE) (= fence currentFence)) (ReentrantFencedMutex. client 2 currentFence highestObservedFence highestObservedFenceOwner)
                     :else (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (cond (= lockCount 1) (ReentrantFencedMutex. nil 0 INVALID_FENCE highestObservedFence owner)
                           :else (ReentrantFencedMutex. owner 1 currentFence highestObservedFence highestObservedFenceOwner)))))))

  Object
  (toString [this] (str "owner: " owner " lock count: " lockCount " lock fence: " currentFence " highest observed fence: " highestObservedFence " highest observed fence owner: " highestObservedFenceOwner)))

(defn createReentrantFencedMutex []
  "A reentrant fenced mutex responding to :acquire and :release messages and tracking monotonicity of observed fences"
  (ReentrantFencedMutex. nil 0 INVALID_FENCE INVALID_FENCE nil))



(defrecord AcquiredPermitsModel [acquired]
  Model
  (step [this op]
    (let [client (getClient op)]
      (if (nil? client)
        (do
          (info "no owner!")
          (knossos.model/inconsistent "no owner!"))
        (condp = (:f op)
          :acquire (if (< (reduce + (vals acquired)) NUMBER_OF_PERMITS)
                     (AcquiredPermitsModel. (assoc acquired client (+ (get acquired client) 1)))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (> (get acquired client) 0)
                     (AcquiredPermitsModel. (assoc acquired client (- (get acquired client) 1)))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))))))

  Object
  (toString [this] (str "acquired: " acquired)))

(defn createAcquiredPermitsModel []
  "A model that assign permits to multiple nodes via :acquire and :release messages"
  (AcquiredPermitsModel. {"n1" 0 "n2" 0 "n3" 0 "n4" 0 "n5" 0}))


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
  {:crdt-map                     (map-workload {:crdt? true})
   :map                          (map-workload {:crdt? false})
   :lock                         {:client    (lock-client "jepsen.lock")
                                  :generator (->> [{:type :invoke, :f :acquire}
                                                   {:type :invoke, :f :release}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 1/10))
                                  :checker   (checker/linearizable)
                                  :model     (model/mutex)}
   :lock-no-quorum               {:client    (lock-client "jepsen.lock.no-quorum")
                                  :generator (->> [{:type :invoke, :f :acquire}
                                                   {:type :invoke, :f :release}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 1/10))
                                  :checker   (checker/linearizable)
                                  :model     (model/mutex)}
   :non-reentrant-cp-lock        {:client    (fenced-lock-client "jepsen.cpLock1")
                                  :generator (->> [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 1/10))
                                  :checker   (checker/linearizable)
                                  :model     (createOwnerAwareMutex)}
   :reentrant-cp-lock            {:client    (fenced-lock-client "jepsen.cpLock2")
                                  :generator (->> [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 1/10))
                                  :checker   (checker/linearizable)
                                  :model     (createReentrantMutex)}
   :non-reentrant-fenced-lock    {:client    (fenced-lock-client "jepsen.cpLock1")
                                  :generator (->> [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 1/10))
                                  :checker   (checker/linearizable)
                                  :model     (createFencedMutex)}
   :reentrant-fenced-lock        {:client    (fenced-lock-client "jepsen.cpLock2")
                                  :generator (->> [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 1/10))
                                  :checker   (checker/linearizable)
                                  :model     (createReentrantFencedMutex)}
   :cp-semaphore {:client        (cp-semaphore-client)
                                  :generator (->> [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 1/10))
                                  :checker   (checker/linearizable)
                                  :model     (createAcquiredPermitsModel)}
   :cp-id-gen-long               {:client    (cp-atomic-long-id-client nil nil)
                                  :generator (->> {:type :invoke, :f :generate}
                                                (gen/stagger 0.5))
                                  :checker   (checker/unique-ids)}
   :cp-cas-long                  {:client    (cp-cas-long-client nil nil)
                                  :generator (->> (gen/mix [{:type :invoke, :f :read}
                                                            {:type :invoke, :f :write, :value (rand-int 5)}
                                                            (gen/sleep 1)
                                                            {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]}])
                                                gen/each
                                                (gen/stagger 0.5))
                                  :checker   (checker/linearizable)
                                  :model     (model/cas-register 0)}
   :cp-cas-reference             {:client    (cp-cas-reference-client nil nil)
                                  :generator (->> (gen/mix [{:type :invoke, :f :read}
                                                            {:type :invoke, :f :write, :value (rand-int 5)}
                                                            (gen/sleep 1)
                                                            {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]}])
                                                  gen/each
                                                  (gen/stagger 0.5))
                                  :checker   (checker/linearizable)
                                  :model     (model/cas-register 0)}
   :queue                        (assoc (queue-client-and-gens)
                                   :checker (checker/total-queue))
   :atomic-ref-ids               {:client    (atomic-ref-id-client nil nil)
                                  :generator (->> {:type :invoke, :f :generate}
                                                  (gen/stagger 0.5))
                                  :checker   (checker/unique-ids)}
   :atomic-long-ids              {:client    (atomic-long-id-client nil nil)
                                  :generator (->> {:type :invoke, :f :generate}
                                                  (gen/stagger 0.5))
                                  :checker   (checker/unique-ids)}
   :id-gen-ids                   {:client    (id-gen-id-client nil nil)
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
                       (gen/nemesis (gen/start-stop 20 20))
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
           {:name      (str "hazelcast " (name (:workload opts)))
            :os        debian/os
            :db        (db)
            :client    client
            :nemesis   (nemesis/partition-majorities-ring)
            ;:nemesis    nemesis/noop
            :generator generator
            :checker   (checker/compose
                         {:perf     (checker/perf)
                          :timeline (timeline/html)
                          :workload checker})
            :model     model})))

(def opt-spec
  "Additional command line options"
  [[nil "--workload WORKLOAD" "Test workload to run, e.g. atomic-long-ids."
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of (workloads)))
    :validate [(workloads) (cli/one-of (workloads))]]])

(defn -main
  "Command line runner."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  hazelcast-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
