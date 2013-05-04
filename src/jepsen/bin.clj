(ns jepsen.bin
  (:require clojure.stacktrace)
  (:use jepsen.set-app
        jepsen.riak
        jepsen.mongo
        jepsen.redis
        jepsen.pg
        [clojure.tools.cli :only [cli]]))

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
   "pg"                     pg-app
   "lock"                   locking-app})

(defn parse-args
  [args]
  (cli args
       ["-n" "--number" "number of elements to add"
        :parse-fn #(Integer. %)]))

(defn -main
  [& args]
  (try
    (let [[opts app-names usage] (parse-args args)
          n (get opts :number 2000)]
      (when (empty? app-names)
        (println usage)
        (println "Available apps:")
        (dorun (map println (sort (keys app-map))))
        (System/exit 0))

      (let [app-fn (->> app-names
                     (map app-map)
                     (apply comp))]
        (run n (apps app-fn))
        (System/exit 0)))

    (catch Throwable t
      (.printStackTrace t)
      ;       (clojure.stacktrace/print-cause-trace t)
      (System/exit 1))))
