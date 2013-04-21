(ns jepsen.bin
  (:use jepsen.set-app
        jepsen.riak
        jepsen.mongo
        jepsen.redis))

(defn -main []
  (run (apps mongo-app))
  (System/exit 0))
