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

(def reentrant-lock-acquire-count 2)
(def num-permits 2)
(def invalid-fence 0)

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
                 (assoc op :type :fail :error :cas-failed)))))

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
                 (assoc op :type :fail :error :cas-failed)))))

    (teardown! [this test]
      (.shutdown conn))))

(defn atomic-ref-id-client
  "Generates unique IDs using an AtomicReference"
  [conn atomic-ref]
  (reify client/Client
    (setup! [_ test node]
      (let [conn (connect node)]
        (atomic-ref-id-client conn (.getAtomicReference conn "jepsen.atomic-ref"))))

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
        (id-gen-id-client conn (.getIdGenerator conn "jepsen.id-gen"))))

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

(defn log-ok
  [clientName op fence]
  (do (info (str "ok " clientName " -> " (:value op)))
      (assoc op :type :ok :value {:client clientName :fence fence :uid (:value op)})))

(defn log-fail
  [clientName op]
  (do (info (str "fail " clientName " -> " (:value op)))
      (assoc op :type :fail)))

(defn log-maybe
  [clientName op exception]
  (do (warn (str "maybe " clientName " -> " (:value op) " exception class: " (.getClass exception) " message: " (.getMessage exception)))
      (assoc op :type :info)))

(defn invoke-acquire [clientName lock op]
  (let [fence (.tryLockAndGetFence lock 5000 TimeUnit/MILLISECONDS)]
    (if (not= fence invalid-fence)
      (log-ok clientName op fence)
      (log-fail clientName op))))

(defn fenced-lock-client
  ([lock-name] (fenced-lock-client nil nil lock-name))
  ([conn lock lock-name]
   (reify client/Client
     (setup! [_ test node]
       (let [conn (connect node)]
         (fenced-lock-client conn (.getLock (.getCPSubsystem conn) lock-name) lock-name)))

     (invoke! [this test op]
       (let [clientName (.getName conn)]
         (try
           (info (str " " clientName " -> " op))
           (swap! (:client-uids-to-client-names test) assoc (:value op) clientName)
           (case (:f op)
             :acquire (invoke-acquire clientName lock op)
             :release (do
                        (.unlock lock)
                        (log-ok clientName op invalid-fence)))
           (catch IllegalMonitorStateException _
             (assoc (log-fail clientName op) :error :not-lock-owner))
           (catch IOException e
             (condp re-find (.getMessage e)
               ; This indicates that the Hazelcast client doesn't have a remote
               ; peer available, and that the message was never sent.
               #"Packet is not send to owner address"
               (assoc (log-fail clientName op) :error :client-down)
               (assoc (log-maybe clientName op e) :error :io-exception)))
           (catch Exception e
             (assoc (log-maybe clientName op e) :error :exception)))))

     (teardown! [this test]
       (.terminate (.getLifecycleService conn))))))

(defn cp-semaphore-client
  ([] (cp-semaphore-client nil nil))
  ([conn semaphore]
   (reify client/Client
     (setup! [_ test node]
       (let [conn (connect node)
             sem (.getSemaphore (.getCPSubsystem conn) "jepsen.cpSemaphore")
             _ (.init sem num-permits)]
         (cp-semaphore-client conn sem)))

     (invoke! [this test op]
       (let [clientName (.getName conn)]
         (try
           (info (str clientName " -> " op))
           (swap! (:client-uids-to-client-names test) assoc (:value op) clientName)
           (case (:f op)
             :acquire (if (.tryAcquire semaphore 5000 TimeUnit/MILLISECONDS)
                        (do
                          (log-ok clientName op invalid-fence))
                        (assoc (log-fail clientName op) :debug {:client clientName :uid (:value op)}))
             :release (do
                        (.release semaphore)
                        (log-ok clientName op invalid-fence))
             )
           (catch IllegalArgumentException _
             (assoc (log-fail clientName op) :error :not-permit-owner :debug {:client clientName :uid (:value op)}))
           (catch IOException e
             (condp re-find (.getMessage e)
               ; This indicates that the Hazelcast client doesn't have a remote
               ; peer available, and that the message was never sent.
               #"Packet is not send to owner address"
               (assoc (log-fail clientName op) :error :client-down :debug {:client clientName :uid (:value op)})
               (assoc (log-maybe clientName op e) :error :io-exception :debug {:client clientName :uid (:value op)})))
           (catch Exception e
             (assoc (log-maybe clientName op e) :error :exception :debug {:client clientName :uid (:value op)})))))

     (teardown! [this test]
       (.terminate (.getLifecycleService conn))))))

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



(defn get-client [client-uids-to-client-names-map op]
  (let [val (:value op)]
    (if (map? val) (:client val) (get @client-uids-to-client-names-map (:value op)))))

(defrecord ReentrantMutex [client-uids-to-client-names-map owner lockCount]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (if (and (< lockCount reentrant-lock-acquire-count) (or (nil? owner) (= owner client)))
                     (ReentrantMutex. client-uids-to-client-names-map client (+ lockCount 1))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (ReentrantMutex. client-uids-to-client-names-map (if (= lockCount 1) nil owner) (- lockCount 1)))))))

  Object
  (toString [this] (str "owner: " owner ", lockCount: " lockCount " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-reentrant-mutex [client-uids-to-client-names-map]
  "A single reentrant mutex responding to :acquire and :release messages"
  (ReentrantMutex. client-uids-to-client-names-map nil 0))



(defrecord OwnerAwareMutex [client-uids-to-client-names-map owner]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (if (nil? owner)
                     (OwnerAwareMutex. client-uids-to-client-names-map client)
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (OwnerAwareMutex. client-uids-to-client-names-map nil))))))

  Object
  (toString [this] (str "owner: " owner " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-owner-aware-mutex [client-uids-to-client-names-map]
  "A single non-reentrant mutex responding to :acquire and :release messages and tracking mutex owner"
  (OwnerAwareMutex. client-uids-to-client-names-map nil))



(defn get-fence [op]
  (let [val (:value op)]
    (if (map? val) (:fence val) invalid-fence)))

(defrecord FencedMutex [client-uids-to-client-names-map owner lockFence prevOwner]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op) fence (get-fence op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (cond
                     (some? owner) (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (= fence invalid-fence) (FencedMutex. client-uids-to-client-names-map client lockFence owner)
                     (> fence lockFence) (FencedMutex. client-uids-to-client-names-map client fence owner)
                     :else (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (FencedMutex. client-uids-to-client-names-map nil lockFence owner))))))

  Object
  (toString [this] (str "owner: " owner " lock fence: " lockFence " prev owner: " prevOwner " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-fenced-mutex [client-uids-to-client-names-map]
  "A fenced mutex responding to :acquire and :release messages and tracking monotonicity of observed fences"
  (FencedMutex. client-uids-to-client-names-map nil invalid-fence nil))



(defrecord ReentrantFencedMutex [client-uids-to-client-names-map owner lockCount currentFence highestObservedFence highestObservedFenceOwner]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op) fence (get-fence op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (cond
                     ; if the lock is not held
                     (nil? owner)
                      (cond
                        ; I can have an invalid fence or a fence larger than highestObservedFence
                        (or (= fence invalid-fence) (> fence highestObservedFence))
                        (ReentrantFencedMutex. client-uids-to-client-names-map client 1 fence (max fence highestObservedFence) highestObservedFenceOwner)
                        :else
                        (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
                     ; if the new acquire does not match to the current lock owner, or the lock is already acquired twice, we cannot acquire anymore
                     (or (not= owner client) (= lockCount reentrant-lock-acquire-count)) (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     ; if the lock is acquired without a fence, and the new acquire has no fence or a fence larger than highestObservedFence
                     (= currentFence invalid-fence) (cond (or (= fence invalid-fence) (> fence highestObservedFence))
                                                      (ReentrantFencedMutex. client-uids-to-client-names-map client (+ lockCount 1) fence (max fence highestObservedFence) highestObservedFenceOwner)
                                                    :else
                                                      (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
                     ; if the lock is acquired with a fence, and the new acquire has no fence or the same fence
                     (or (= fence invalid-fence) (= fence currentFence)) (ReentrantFencedMutex. client-uids-to-client-names-map client (+ lockCount 1) currentFence highestObservedFence highestObservedFenceOwner)
                     :else (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (or (nil? owner) (not= owner client))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this))
                     (cond (= lockCount 1) (ReentrantFencedMutex. client-uids-to-client-names-map nil 0 invalid-fence highestObservedFence (if (= currentFence invalid-fence) highestObservedFenceOwner owner))
                           :else (ReentrantFencedMutex. client-uids-to-client-names-map owner (- lockCount 1) currentFence highestObservedFence highestObservedFenceOwner)))))))

  Object
  (toString [this] (str "owner: " owner " lock count: " lockCount " lock fence: " currentFence " highest observed fence: " highestObservedFence " highest observed fence owner: " highestObservedFenceOwner " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-reentrant-fenced-mutex [client-uids-to-client-names-map]
  "A reentrant fenced mutex responding to :acquire and :release messages and tracking monotonicity of observed fences"
  (ReentrantFencedMutex. client-uids-to-client-names-map nil 0 invalid-fence invalid-fence nil))



(defrecord AcquiredPermitsModel [client-uids-to-client-names-map acquired]
  Model
  (step [this op]
    (let [client (get-client client-uids-to-client-names-map op)]
      (if (nil? client)
        (knossos.model/inconsistent "no owner!")
        (condp = (:f op)
          :acquire (if (< (reduce + (vals acquired)) num-permits)
                     (AcquiredPermitsModel. client-uids-to-client-names-map (assoc acquired client (+ (get acquired client) 1)))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))
          :release (if (> (get acquired client) 0)
                     (AcquiredPermitsModel. client-uids-to-client-names-map (assoc acquired client (- (get acquired client) 1)))
                     (knossos.model/inconsistent (str "client: " client " cannot " op " on " this)))))))

  Object
  (toString [this] (str "acquired: " acquired " client-uids-to-client-names-map: " @client-uids-to-client-names-map)))

(defn create-acquired-permits-model [client-uids-to-client-names-map]
  "A model that assign permits to multiple nodes via :acquire and :release messages"
  (AcquiredPermitsModel. client-uids-to-client-names-map {"n1" 0 "n2" 0 "n3" 0 "n4" 0 "n5" 0}))


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
  [client-uids-to-client-names-map]
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
                                                  (gen/stagger 0.5))
                                  :checker   (checker/linearizable)
                                  :model     (create-owner-aware-mutex client-uids-to-client-names-map)}
   :reentrant-cp-lock            {:client    (fenced-lock-client "jepsen.cpLock2")
                                  :generator (->> [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 0.5))
                                  :checker   (checker/linearizable)
                                  :model     (create-reentrant-mutex client-uids-to-client-names-map)}
   :non-reentrant-fenced-lock    {:client    (fenced-lock-client "jepsen.cpLock1")
                                  :generator (->> [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 1))
                                  :checker   (checker/linearizable)
                                  :model     (create-fenced-mutex client-uids-to-client-names-map)}
   :reentrant-fenced-lock        {:client    (fenced-lock-client "jepsen.cpLock2")
                                  :generator (->> [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 1))
                                  :checker   (checker/linearizable)
                                  :model     (create-reentrant-fenced-mutex client-uids-to-client-names-map)}
   :cp-semaphore {:client        (cp-semaphore-client)
                                  :generator (->> [{:type :invoke, :f :acquire :value (.toString (UUID/randomUUID))}
                                                   {:type :invoke, :f :release :value (.toString (UUID/randomUUID))}]
                                                  cycle
                                                  gen/seq
                                                  gen/each
                                                  (gen/stagger 0.5))
                                  :checker   (checker/linearizable)
                                  :model     (create-acquired-permits-model client-uids-to-client-names-map)}
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
  (let [client-uids-to-client-names-map (atom {})
        {:keys [generator
                final-generator
                client
                checker
                model]}
        (get (workloads client-uids-to-client-names-map) (:workload opts))
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
            :model     model
            :client-uids-to-client-names client-uids-to-client-names-map})))

(def opt-spec
  "Additional command line options"
  [[nil "--workload WORKLOAD" "Test workload to run, e.g. atomic-long-ids."
    :parse-fn keyword
    :missing (str "--workload " (cli/one-of (workloads nil)))
    :validate [(workloads nil) (cli/one-of (workloads nil))]]])

(defn -main
  "Command line runner."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  hazelcast-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
