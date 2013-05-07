(ns jepsen.redis
  (:use [clojure.set :only [union difference]]
        jepsen.set-app)
  (:require [taoensso.carmine :as redis]
            [clojure.string :as string]))

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

(defn redis-app
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
