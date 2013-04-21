(ns jepsen.bin
  (:use jepsen.set-app
        jepsen.riak))

(defn -main []
  (run (apps (comp locking-app riak-crdt-app)))
  (System/exit 0))
