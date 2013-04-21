(ns jepsen.riak
  (:use [clojure.set :only [union difference]]
        jepsen.set-app)
  (:require [clojurewerkz.welle.core :as welle]
            [clojurewerkz.welle.kv :as kv]
            [clojurewerkz.welle.buckets :as buckets])
  (:import (com.basho.riak.client.cap Quora
                                      ConflictResolver)))

(defn riak-resolver
  "Defines a riak conflict resolver. Takes a default new object, and a function
  which takes a seq of objects, and returns one object."
  [default f]
  (reify ConflictResolver
    (resolve [_ siblings]
             (locking *out* (prn (count siblings) "siblings")
               (doseq [v (map :vclock siblings)]
                 (println (.asString v))))
             (case (count siblings)
               0 [default]
               1 siblings
               [(f siblings)]))))

(defn riak-value-resolver
  "Constructs a resolver based on a default new value, and a function which
  takes a seq of values and returns a resolved value."
  [default f]
  (riak-resolver {:content-type "application/clojure"
                  :metadata {}
                  :value default}
                 (fn wrapper [siblings]
                   (-> siblings
                     first
                     (select-keys [:metadata :content-type :indexes :links])
                     (assoc :value (f (map :value siblings)))))))
                 
(def resolver
  (riak-value-resolver
    #{}
;    #(apply union (map set %))))
    #(apply union %)))

(defn riak-app
  [opts]
  (let [bucket     (get opts :bucket "test")
        key        (get opts :key "test")
        host       (get opts :host "127.0.0.1")
        port       (get opts :port 8087)
        client     (welle/connect-via-pb host port)
        read-opts  (mapcat identity (:read opts))
        write-opts (mapcat identity (:write opts))
        bucket-opts (get opts :bucket-opts {})]

    (reify SetApp
      (setup [app]
             (welle/with-client client
                                ; Reset bucket
                                (apply buckets/update bucket
                                       (mapcat identity bucket-opts))
                                (let [o (buckets/fetch bucket)]
                                  (assert
                                    (every? (fn [[k v]]
                                              (= v (get o k)))
                                            bucket-opts)))
                  
                                ; Nuke record
                                (kv/delete bucket key :dw Quora/ALL)
                                (assert (empty? (kv/fetch bucket key
                                                          :pr Quora/ALL)))))

      (add [app element]
           (let [res (-> (future
                           (welle/with-client
                             client
                             (apply kv/modify bucket key
                                    (fn [v]
                                      (locking *out* (prn v))
                                      (update-in v [:value] conj element))
                                    (concat read-opts write-opts))))
                           (deref 5000 ::timeout))]
             (when (= res ::timeout)
;               (println "Timed out.")
               (throw (RuntimeException. "timeout")))))


      (results [app]
               (welle/with-client client
                                  (-> (apply kv/fetch bucket key
                                           :pr Quora/ALL
                                           read-opts)
                                    prn
                                    first
                                    :value)))

      (teardown [app]
                (welle/with-client
                  client
                  ; Wipe object
;                  (kv/delete bucket key :dw Quora/ALL)
;                  (assert (empty? (kv/fetch bucket key
;                                            :pr Quora/ALL))))))))
                  )))))

(defn riak-lww-all-app
  [opts]
  (riak-app (merge {:read {:r Quora/ALL
                           :pr Quora/ALL}
                    :write {:w Quora/ALL
                            :dw Quora/ALL
                            :pw Quora/ALL}
                    :bucket-opts {:allow-siblings false
;                                  :last-write-wins true
                                  :n-val 3}}
                   opts)))

(defn riak-lww-quorum-app
  [opts]
  (riak-app (merge {:read {:r Quora/QUORUM
                           :pr Quora/QUORUM}
                    :write {:w Quora/QUORUM
                            :dw Quora/QUORUM
                            :pw Quora/QUORUM}
                    :bucket-opts {:allow-siblings false
                                  :n-val 3}}
                   opts)))

(defn riak-lww-sloppy-quorum-app
  [opts]
  (riak-app (merge {:read {:r Quora/QUORUM}
                    :write {:w Quora/QUORUM
                            :dw Quora/QUORUM}
                    :bucket-opts {:allow-siblings false
                                  :n-val 3}}
                   opts)))

(defn riak-crdt-app
  [opts]
  (riak-app (merge {:read {:r Quora/ALL
                           :resolver resolver}
                    :write {:w Quora/ALL}
                    :bucket-opts {:allow-siblings true
                                  :n-val 3}}
                   opts)))
