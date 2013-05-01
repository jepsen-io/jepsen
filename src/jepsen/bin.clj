(ns jepsen.bin
  (:require clojure.stacktrace)
  (:use jepsen.set-app
        jepsen.riak
        jepsen.mongo
        jepsen.redis
        jepsen.pg))

(def app-map
  "A map from command-line names to apps you can run"
  {"mongo-replicas-safe"    mongo-replicas-safe-app
   "mongo-safe"             mongo-safe-app
   "mongo-unsafe"           mongo-unsafe-app
   "mongo"                  mongo-app
   "redis"                  redis-app
   "riak-lww-all"           riak-lww-all-app
   "riak-lww-quorum"        riak-lww-quorum-app
   "riak-lww-sloppy-quorum" riak-lww-sloppy-quorum-app
   "riak-crdt"              riak-crdt-app
   "pg"                     pg-app})

(defn -main
  ([]
   (println "Available apps:")
   (dorun (map println (keys app-map))))
  ([app]
   (try
     (run (apps (app-map app)))
     (System/exit 0)
     (catch Throwable t
       (.printStackTrace t)
;       (clojure.stacktrace/print-cause-trace t)
       (System/exit 1)))))
