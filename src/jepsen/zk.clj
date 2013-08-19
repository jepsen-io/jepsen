(ns jepsen.zk
  "Zookeeper test. Mostly copied from a curator wrapper that's been slowly
  accreting--really need to make that into a library some day."
  (:use
    jepsen.util
    jepsen.set-app
    jepsen.load)
  (:require
    [clojure.edn :as edn]
    [clojure.tools.logging :as log]
    [clojure.set :as set])
  (:import
    (clojure.lang IDeref
                  IRef)
    (java.util.concurrent TimeUnit)
    (java.io ByteArrayInputStream
             InputStreamReader
             PushbackReader)
    (org.apache.curator.framework CuratorFrameworkFactory
                                  CuratorFramework)
    (org.apache.curator.framework.api CuratorWatcher
                                      PathAndBytesable
                                      Versionable)
    (org.apache.curator.framework.state ConnectionStateListener
                                        ConnectionState)
    (org.apache.curator.framework.imps CuratorFrameworkState)
    (org.apache.curator.retry BoundedExponentialBackoffRetry)
    (org.apache.curator.framework.recipes.locks InterProcessMutex)
    (org.apache.curator.framework.recipes.atomic DistributedAtomicValue
                                                 PromotedToLock)
    (org.apache.curator.framework.recipes.shared SharedValue
                                                 SharedValueListener)
    (org.apache.zookeeper KeeperException
                          KeeperException$NoNodeException
                          KeeperException$NodeExistsException
                          KeeperException$BadVersionException)
    (org.apache.zookeeper.data Stat)))

(defn encode
  "Serialize an object to bytes."
  [o]
  (if (nil? o)
    (byte-array 0)
    (binding [*print-dup* false]
      (-> o pr-str .getBytes))))

(defn decode
  "Deserialize bytes to an object."
  [^bytes bytes]
  (if (or (nil? bytes) (= 0 (alength bytes)))
    nil
    (with-open [s (ByteArrayInputStream. bytes)
                i (InputStreamReader. s)
                r (PushbackReader. i)]
      (binding [*read-eval* false]
        (edn/read r)))))

(defn retry-policy
  "A bounded exponential backoff retry policy."
  []
  (BoundedExponentialBackoffRetry. 1000 3000 2))

(defn ^CuratorFramework framework
  "Returns a new Curator framework with the given ZK connection string and
  namespace."
  [zk-connect-string namespace]
  (doto
    (.. (CuratorFrameworkFactory/builder)
        (namespace namespace)
        (connectString zk-connect-string)
        (sessionTimeoutMs 2000)
        (connectionTimeoutMs 2000)
        (retryPolicy (retry-policy))
        (build))
    .start))

(defonce ^CuratorFramework curator nil)

(defn create!
  "Create a znode."
  ([^CuratorFramework curator path]
   (.. curator create creatingParentsIfNeeded (forPath path)))
  ([^CuratorFramework curator path data]
   (.. curator create creatingParentsIfNeeded (forPath (encode data)))))

(defn delete!
  "Ensures a znode does not exist. Idempotent--will not throw if the znode
  doesn't exist already."
  ([^CuratorFramework curator path]
   (try
     (.. curator delete (forPath path))
     (catch KeeperException$NoNodeException e nil))))

(defn interrupt-when-lost
  "Returns a ConnectionStateListener which interrupts the current thread when
  the connection transitions to LOST."
  []
  (let [thread      (Thread/currentThread)
        interrupter (delay (.interrupt thread))]
    (reify ConnectionStateListener
      (stateChanged [this client state]
        (condp = state
          ConnectionState/LOST      @interrupter
          ConnectionState/SUSPENDED nil ;; maybe interrupt here too?
          nil)))))

(defmacro with-curator-listener
  "Registers a curator state listener for the duration of the body."
  [^CuratorFramework curator listener & body]
  `(let [l# (.getConnectionStateListenable curator)
         listener# ~listener]
     (try
       (.addListener l# listener#)
       ~@body
       (finally
         (.removeListener l# listener#)))))

(defmacro with-lock
  "Acquires a distributed lock on a path, then evaluates body with the lock
  held. Always releases the lock when at the end of the body. If acquisition
  fails, throws. If the lock is lost during execution of the body, will
  interrupt the thread which invoked locking-zk."
  [^CuratorFramework curator path wait-ms & body]
  `(let [path# ~path
         lock# (InterProcessMutex. curator path#)]
     (with-curator-listener curator (interrupt-when-lost)
       (when-not (.acquire lock# ~wait-ms TimeUnit/MILLISECONDS)
         (throw (IllegalStateException. (str "Failed to lock " path#))))
       (try
         ~@body
         (finally
           (try
             (.release lock#)
             ; If we lost the session, our znode will be deleted for us
             (catch IllegalStateException e# nil)))))))

(defprotocol Atomic
  "Protocol for atomic Zookeeper operations."
  (swap- [this f args])
  (reset!! [this value]))

(defn distributed-atom
  "Creates a distributed atom at a given path. Takes an initial value which
  will be set only if the znode does not exist yet."
  [^CuratorFramework curator path initial-value]
  ; Initial value
  (try
    (.. curator
        create
        creatingParentsIfNeeded
        (forPath path (encode initial-value)))
    (catch KeeperException$NodeExistsException e))

  ; Create distributed atomic value
  (let [dav (DistributedAtomicValue.
              curator
              path
              (retry-policy)
              (.. (PromotedToLock/builder)
                  (lockPath path)
                  (retryPolicy (retry-policy))
                  build))]

    ; Wrap it in our protocols
    (reify
      IDeref
      (deref [this]
        (decode (.postValue (.get dav))))

      Atomic
      (swap- [this f args]
        (loop [value @this]
          (let [value' (apply f value args)
                result (.compareAndSet dav (encode value) (encode value'))]
            (if (.succeeded result)
              (decode (.postValue result))
              (do
                ; Randomized backoff might be useful if we use optimistic
                ; concurrency instead of lock promotion
                ;                (Thread/sleep (rand-int 10))
                (recur (decode (.preValue result))))))))

      (reset!! [this value]
        (let [encoded-value (encode value)]
          (loop []
            (if (.succeeded (.trySet dav encoded-value))
              value
              (recur))))))))

(declare update-shared-atom-)
(declare refresh-shared-atom!)

(deftype SharedAtom
  [^CuratorFramework curator
   path
   state     ; Atom of {:state :value :version :watches}
   listener  ; Atom to a connection state listener
   watcher]  ; Atom to a ZK watcher for the object

  IDeref
  (deref [this]
    (:value @state))

  IRef
  (getValidator [this]     (constantly true))
  (getWatches   [this]     (:watches @state))
  (addWatch     [this k f] (do (swap! state update-in [:watches] assoc k f)
                               this))
  (removeWatch  [this k]   (do (swap! state update-in [:watches] dissoc k)
                               this))

  Atomic
  (reset!! [this value]
    ; Possible race condition: doesn't respect shutdown correctly.
    (assert (= :started (:state @state)))

    ; Update value in ZK
    (.. curator setData (forPath path (encode value)))
    (refresh-shared-atom! this))

  (swap- [this f args]
    ; Possible race condition: doesn't respect shutdown correctly.
    (assert (= :started (:state @state)))

    (let [res (try
                (let [{:keys [value version]} @state
                      value  (apply f value args)
                      ; Write new value
                      ^Stat stat (.. curator
                                     setData
                                     (withVersion version)
                                     (forPath path (encode value)))]
                  ; Broadcast changes
                  (update-shared-atom- this (.getVersion stat) value)
                  value)

                ; If that update process fails, return ::bad-version instead.
                (catch KeeperException$BadVersionException e ::bad-version))]
      (if (= ::bad-version res)
        (do
          ; Back off, re-read, and retry.
          (Thread/sleep (rand-int 10))
          (refresh-shared-atom! this)
          (recur f args))
        res))))

(defn update-shared-atom-
  "Updates a shared atom with a new value. This broadcasts changes locally--it
  doesn't send updates back out to ZK. You probably don't want to call this
  unless you really know what you're doing. Returns the latest value."
  [^SharedAtom a version' value']
  (assert version')
  (locking a
    (let [old (atom nil)
          new (swap! (.state a)
                     (fn [state]
                       (if (< (:version state) version')
                         ; Allow monotonic updates
                         (do (reset! old state)
                             (merge state {:version version'
                                           :value value'}))
                         ; Otherwise, ignore
                         (do (reset! old nil)
                             state))))
          old @old]

      (when old
        ; We advanced; call watches
        (doseq [[k f] (:watches new)]
          (f k a (:value old) (:value new))))

      (:value new))))

(defn refresh-shared-atom!
  "Reads the most recent value for a SharedAtom, re-establishing a watch.
  Returns the latest value."
  [^SharedAtom a]
  (locking a
    (let [stat (Stat.)
          bytes (.. (.curator a)
                    getData
                    (storingStatIn stat)
                    (usingWatcher (deref (.watcher a)))
                    (forPath (.path a)))]
      (update-shared-atom- a (.getVersion stat) (decode bytes)))))

(defn shutdown-shared-atom
  "Call when you're done using a SharedAtom."
  [^SharedAtom a]
  ; Stop listening for connection updates.
  (.. a curator getConnectionStateListenable (removeListener @(.listener a)))
  (reset! (.state a) {:state :closed}))

(defn shared-atom-listener
  "Watches the connection state of Curator for a SharedAtom."
  [a]
  (reify ConnectionStateListener
    (stateChanged [this client state]
      (condp = state
        ConnectionState/READ_ONLY   nil
        ConnectionState/CONNECTED   (refresh-shared-atom! a)
        ConnectionState/RECONNECTED (refresh-shared-atom! a)
        ConnectionState/LOST        nil
        ConnectionState/SUSPENDED   nil
        nil))))

(defn shared-atom-watcher
  "A curator watcher which refreshes the SharedAtom atom when changes occur."
  [^SharedAtom a]
  (reify CuratorWatcher
    (process [this event]
      (when (= :started (-> a .state deref :state))
        (refresh-shared-atom! a)))))

(defn shared-atom
  "Creates a shared atom at a given path. Takes an initial value which will be
  set only if the znode does not exist yet. Unlike distributed-atom,
  shared-atom does not use lock promotion; all writes are optimistic with
  exponential backoff. Reads are cheaper because they can use a locally cached
  value. Also unlike distributed-atom, shared-atom is watchable for changes.

  You must explicitly shut down a shared-atom using shutdown-shared."
  [^CuratorFramework curator path initial-value]

  (let [curator curator
        ; Initialize value if nonexistent
        _ (try
            (.. curator
                create
                creatingParentsIfNeeded
                (forPath path (encode initial-value)))
            (catch KeeperException$NodeExistsException e))

        ; Create shared atom structure
        a (SharedAtom. curator
                       path
                       (atom {:state :started
                              :version -1
                              :value nil
                              :watches {}})
                       (atom nil)
                       (atom nil))]

    ; Hook up watchers and listeners
    (reset! (.listener a) (shared-atom-listener a))
    (reset! (.watcher a)  (shared-atom-watcher a))

    ; Do an initial read
    (refresh-shared-atom! a)

    a))

(defn swap!!
  [atomic f & args]
  (swap- atomic f args))

(defn zk-app
  [opts]
  (let [curator (framework (str (:host opts) ":2181") "jepsen")
        path    "/set-app"
        state   (distributed-atom curator path [])]

    (reify SetApp
      (setup [app]
        (reset!! state []))

      (add [app element]
        (try
          (swap!! state conj element)
          ok
          (catch org.apache.zookeeper.KeeperException$ConnectionLossException e
            error)))

      (results [app]
        @state)

      (teardown [app]
        (delete! curator path)))))
