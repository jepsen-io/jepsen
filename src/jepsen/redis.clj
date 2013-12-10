(ns jepsen.redis
  (:use [clojure.set :only [union difference]]
        jepsen.util
        jepsen.set-app
        jepsen.load)
  (:require [taoensso.carmine :as redis]
            [clojure.string :as string]
            [jepsen.failure :as failure]
            [jepsen.control.net :as control.net]
            [jepsen.control :as control]))

(defmacro with-node
  "Given a hostname, creates a new client/pool for that host and evaluates body
  with that pool."
  [host & body]
  `(let [pool# (redis/make-conn-pool)
         spec# (redis/make-conn-spec :host ~host)]
     (redis/with-conn pool# spec#
       ~@body)))

(defn info
  "Returns a map, keyed by keywords, of redis state. I can't figure out how the
  heck carmine's info works."
  [node]
  (->> (string/split
         (with-node node
           (taoensso.carmine.protocol/send-request! "INFO"))
         #"\r\n")
       (remove (partial re-find #"^#"))     ; Drop comments
       (remove (partial re-matches #"\s*")) ; Drop blanks
       (map (fn [line]
              (let [[k v] (string/split line #":" 2)]
                ; Keywordize key, and maybe parse numbers, ugh, hack
                [(keyword k)
                 (if (re-matches #"-?\d+(\.\d+)?" v)
                   (read-string v)
                   v)])))
       (into {})))

(defn repl-offset
  "The replication offset for a node."
  [node]
  (let [info (info node)]
    (if (= "master" (:role info))
      (:master_repl_offset info)
      (:slave_repl_offset info))))

(defn highest-node
  "Which node has the highest replication offset?"
  [nodes]
  (log (map repl-offset nodes))
  (->> nodes
       (reduce (fn [[highest offset] node]
                 (let [offset' (repl-offset node)]
                   (if (or (nil? highest)
                           (< offset offset'))
                     [node offset']
                     [highest offset])))
               nil)
       first))

(defn elect!
  "Forces an election among the given nodes. Picks the node with the highest
  replication offset, promotes it, and re-parents the secondaries."
  [nodes]
  (let [highest (highest-node nodes)]
    (log "Promoting" highest)
    (with-node highest
      (redis/slaveof "no" "one"))
    (doseq [node (remove #{highest} nodes)]
      (log "Reparenting" node "to" highest)
      (with-node node
        (redis/slaveof highest 6379)))))

(defn master
  "Given a sentinel pool and spec, finds the current master from that
  sentinel's perspective, or blocks until it is available. Returns a client
  spec."
  [pool spec]
  (let [[host port] (redis/with-conn pool spec
                                     (taoensso.carmine.protocol/send-request!
                                       "SENTINEL" "get-master-addr-by-name"
                                       "mymaster"))]
    (redis/make-conn-spec :host host :port (Integer. port))))

(defn sentinel
  "Redis sentinel state."
  [spec]
  [(redis/make-conn-pool) spec])

(defmacro with-sentinel
  "Implements the redis sentinel protocol for handling write errors."
  [sentinel & body]
  `(let [[pool# sentinel-spec#] ~sentinel
         spec# (master pool# sentinel-spec#)]
     (redis/with-conn pool# spec# ~@body)))
;     (catch Exception e#
  ;       (case (first (string/split (.getMessage e#) #" "))
  ;         "READONLY" (reset! client-spec#
  ;                            (master sentinel-pool sentinel-spec))))))

(defn sentinel-app
  [opts]
  (let [key      (get opts :key "test")
        sentinel (sentinel
                   (redis/make-conn-spec
                        :host (:host opts)
                        :port 26379))]
    (reify SetApp
      (setup [app]
             (with-sentinel sentinel
                            (redis/del key)))

      (add [app element]
           (with-sentinel sentinel
                          (redis/sadd key element)
                          (Thread/sleep 100)))

      (results [app]
               (set (map #(Long. %)
                         (with-sentinel sentinel
                                        (redis/smembers key)))))

      (teardown [app]
                (with-sentinel sentinel
                               (redis/del key))))))

(defn wait-app
  "Uses WAIT to attempt linearizability."
  [opts]
  (let [key (get opts :key "test")
        spec (redis/make-conn-spec :host (:host opts))
        pool (redis/make-conn-pool)]

    (reify SetApp
      (setup [app]
        (teardown app)
        (elect! ["n1" "n2" "n3" "n4" "n5"]))

      (add [app element]
        (try
          (redis/with-conn pool spec
            (redis/sadd key element))
          ; Block for 2 secondaries (3 total) to ack.
          (let [acks (redis/with-conn pool spec
                       (taoensso.carmine.protocol/send-request! "WAIT" 2 1000))]
            (if (< acks 2)
              (do
                (log "not enough copies: " acks)
                error)
              ok))
          (catch Exception e
            (if (->> e .getMessage (re-find #"^READONLY"))
              error
              (throw e)))))
      
      (results [app]
        (set (map #(Long. %)
                  (redis/with-conn pool spec
                    (redis/smembers key)))))

      (teardown [app]
        (try
          (redis/with-conn pool spec
            (redis/del key))
          (catch Exception e))))))

(def failure
  (reify failure/Failure
    (fail [_ nodes]
      ; Partition nodes
      (log "Isolating [n1 n2] from [n3 n4 n5]")
      (control/on-many nodes (control.net/partition))

      ; Elect new leaders in majority component
      (elect! ["n3" "n4" "n5"])

      (Thread/sleep 5000)

      (log (highest-node nodes)))

    (recover [_ nodes]
      ; Recover partition
      (control/on-many nodes (control.net/heal))
      (log "Partition healed.")

      ; Elect new leaders globally.
      (elect! nodes)

      (Thread/sleep 10000)
      (log (highest-node nodes)))))

