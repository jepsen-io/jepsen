(ns jepsen.bin
  (:require clojure.stacktrace
            [jepsen.cassandra :as cassandra]
            [jepsen.kafka :as kafka]
            [jepsen.failure :as failure])
  (:use jepsen.set-app
        [jepsen.cassandra :only [cassandra-app]]
        [jepsen.riak :only [riak-lww-all-app
                            riak-lww-quorum-app
                            riak-lww-sloppy-quorum-app
                            riak-crdt-app]]
        jepsen.mongo
        jepsen.redis
        [jepsen.pg    :only [pg-app]]
        [jepsen.nuodb :only [nuodb-app]]
        [jepsen.zk    :only [zk-app]]
        [clojure.tools.cli :only [cli]]))

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
   "redis"                  redis-app
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
   "kafka"      kafka/failure})

(defn parse-int [i] (Integer. i))

(defn parse-args
  [args]
  (cli args
       ["-n" "--number" "number of elements to add" :parse-fn parse-int]
       ["-r" "--rate" "requests per second" :parse-fn parse-int]
       ["-f" "--failure" "failure mode"]))

(defn -main
  [& args]
  (try
    (let [[opts app-names usage] (parse-args args)
          failure (-> opts
                      (get :failure "partition")
                      failures)
          n (get opts :number 2000)
          r (get opts :rate 2)]

      (when (empty? app-names)
        (println usage)
        (println "Available apps:")
        (dorun (map println (sort (keys app-map))))
        (System/exit 0))

      (let [app-fn (->> app-names
                        (map app-map)
                        (apply comp))]
        (run r n failure (apps app-fn))
        (System/exit 0)))

    (catch Throwable t
      (.printStackTrace t)
      ;       (clojure.stacktrace/print-cause-trace t)
      (System/exit 1))))
