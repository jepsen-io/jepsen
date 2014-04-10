(ns jepsen.bin
  (:require clojure.stacktrace
            [jepsen.cassandra :as cassandra]
            [jepsen.kafka :as kafka]
            [jepsen.failure :as failure]
            [jepsen.redis :as redis])
  (:use jepsen.set-app
        [jepsen.cassandra :only [cassandra-app]]
        [jepsen.riak :only [riak-lww-all-app
                            riak-lww-quorum-app
                            riak-lww-sloppy-quorum-app
                            riak-crdt-app
                            ]]
        jepsen.mongo
        [jepsen.pg    :only [pg-app]]
        [jepsen.nuodb :only [nuodb-app]]
        [jepsen.zk    :only [zk-app]]
        [clojure.tools.cli :only [cli]]
        [jepsen.control :only [*password*]]
        ))

(def app-map
  "A map from command-line names to apps you can run"
  {"cassandra"              cassandra-app
   "cassandra-counter"      cassandra/counter-app
   "cassandra-set"          cassandra/set-app
   "cassandra-isolation"    cassandra/isolation-app
   "cassandra-transaction"  cassandra/transaction-app
   "cassandra-transaction-dup" cassandra/transaction-dup-app
   "kafka"                  kafka/app
   "mongo-replicas-safe"    mongo-replicas-safe-app
   "mongo-safe"             mongo-safe-app
   "mongo-unsafe"           mongo-unsafe-app
   "mongo"                  mongo-app
   "redis-sentinel"         redis/sentinel-app
   "redis-wait"             redis/wait-app
   "riak-lww-all"           riak-lww-all-app
   "riak-lww-quorum"        riak-lww-quorum-app
   "riak-lww-sloppy-quorum" riak-lww-sloppy-quorum-app
   "riak-crdt"              riak-crdt-app
   "pg"                     pg-app
   "nuodb"                  nuodb-app
   "zk"                     zk-app
   "lock"                   locking-app})

(def failures
  "A map from command-line names to failure modes."
  {"partition"  failure/simple-partition
   "noop"       failure/noop
   "chaos"      (failure/chaos)
   "kafka"      kafka/failure
   "redis"      redis/failure})

(defn parse-int [i] (Integer. i))

(defn parse-args
  [args]
  (cli args
       ["-n" "--number" "number of elements to add" :parse-fn parse-int]
       ["-r" "--rate" "requests per second" :parse-fn parse-int]
       ["-f" "--failure" "failure mode"]
       ["-X" "--special" "additional app arguments (if supported)"]
       ["-u" "--username" "username to use for ssh"]
       ["-p" "--password" "password for sudo invocations"]
       ["-k" "--key_path" "path to private ssh key"]
       ["-t" "--port" "port to use if not using the default"]
       ))

(defn -main
  [& args]
  (try
    (let [[opts app-names usage] (parse-args args)
          failure (-> opts
                      (get :failure "partition")
                      failures)
          n (get opts :number 2000)
          r (get opts :rate 2)
          spex  (get opts :special [])
          uname (get opts :username "ubuntu")
          pw    (get opts :password nil)
          private-key-path (get opts :key_path nil)
          port  (get opts :port nil)
          ]

      (when (or (empty? app-names)
                (not-every? true? (map (partial contains? app-map) app-names)))
        (println usage)
        (println "Available apps:")
        (dorun (map println (sort (keys app-map))))
        (System/exit 0))

      (when
        (some #{"foundationdb"
                "foundationdb-append"
                "foundationdb-append-noretry"} app-names)
        (require 'jepsen.foundationdb)
        (def app-map (merge app-map {
          "foundationdb" (eval 'jepsen.foundationdb/foundationdb-app)
          "foundationdb-append" (eval 'jepsen.foundationdb/foundationdb-append-app)
          "foundationdb-append-noretry" (eval 'jepsen.foundationdb/foundationdb-append-noretry-app)
          })))

      (with-redefs [jepsen.control/*password* pw
                    jepsen.control/*username* uname
                    jepsen.control/*private-key-path* private-key-path]
        (let [app-fn (->> app-names
                          (map app-map)
                          (apply comp))] 
          (run r n failure (apps app-fn port spex))
          (System/exit 0))
        ))

    (catch Throwable t
      (.printStackTrace t)
      ;       (clojure.stacktrace/print-cause-trace t)
      (System/exit 1))))
