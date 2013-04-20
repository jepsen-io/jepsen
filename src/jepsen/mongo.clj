(ns jepsen.mongo
  (:use [clojure.set :only [union difference]]
        [monger.result :only [ok? has-error?]]
        jepsen.set-app)
  (:require [monger.core :as mg]
            [monger.collection :as c]
            [clojure.string :as string])
  (:import (com.mongodb WriteConcern)))

(defn cas
  "Compare-and-set for a single mongo object. Takes a transformation function f
  which is called with the current value of the object k in the given
  collection:
  
  (f current-object-value arg1 arg2 ...)
  
  and sets the object to the return value of f. Spins until the write
  succeeds. Returns the final value of the document."
  [coll query write-concern f & args]
  (loop [i 0]
    (let [obj (first (c/find-maps coll query))
                obj' (apply f obj args)
                res (c/update coll obj obj'
                              :write-concern write-concern)]
            (if (pos? (.getN res))
              obj'
              ; Conflicted with another write
              (do
                ; Random backoff
                (Thread/sleep (rand 10))
                (recur (inc i)))))))

(defn mongo-app
  [opts]
  (let [coll    (get opts :coll "set-app")
        key     (get opts :key "test")
        concern (get opts :write-concern WriteConcern/MAJORITY)
        address (map #(mg/server-address % 27017)
                     (:hosts opts))
        opts    (mg/mongo-options)
        conn    (mg/connect address opts)]

    ; Set DB
    (mg/with-connection conn
      (mg/set-db! (mg/get-db (get opts :db "test"))))

    (reify SetApp
      (setup [app]
             (mg/with-connection conn
                              (c/remove coll)
                              (c/insert coll {:key key :elements []})))

      (add [app element]
           (mg/with-connection
             conn
             (let [res (cas coll {:key key} concern
                            update-in [:elements] conj element)])))

      (results [app]
               (mg/with-connection conn
                                   (->> {:key key}
                                     (c/find-maps coll)
                                     first
                                     :elements
                                     set)))

      (teardown [app]
             (mg/with-connection conn
                                 (c/remove coll))
                (.close conn)))))

(defn mongo-replicas-safe-app
  [opts]
  (mongo-app (merge {:write-concern WriteConcern/REPLICAS_SAFE} opts)))

(defn mongo-unsafe-app
  [opts]
  (mongo-app (merge {:write-concern WriteConcern/ERRORS_IGNORED} opts)))
