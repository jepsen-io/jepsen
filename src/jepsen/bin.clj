(ns jepsen.bin
  (:use jepsen.set-app
        jepsen.riak))

(defn -main []
  (run (apps riak-crdt-app))
  (System/exit 0))
