(ns jepsen.riak
  (:use [clojure.set :only [union difference]]
        jepsen.set-app)
  (:require [clojurewerkz.welle.core :as welle]
            [clojurewerkz.welle.kv :as kv]
            [clojurewerkz.welle.buckets :as buckets])
  (:import (com.basho.riak.client.cap Quora)))

(defn fetch
  "Fetch a key, resolving conflicts with set union."
  [bucket key & opts]
;  (prn :read opts)
  (let [v (apply kv/fetch bucket key opts)]
;    (prn v)
    (->> v
      (map :value)
      (reduce union #{}))))

(defn store
  "Writes an object as a clojure structure."
  [bucket key value & opts]
;  (prn :write opts)
  (apply kv/store bucket key value
         :content-type "application/clojure"
         opts))

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
                                            bucket-opts)))))

      (add [app element]
           (welle/with-client client
                              (let [set (apply fetch bucket key read-opts)]
                                (apply store bucket key
                                       (conj set element)
                                       write-opts))))

    (results [app]
             (welle/with-client client
                                (fetch bucket key
                                       :pr Quora/ALL)))

  (teardown [app]
            (welle/with-client client
                               ; Wipe object
                               (kv/delete bucket key :dw Quora/ALL)
                               (assert (= #{} (fetch bucket key
                                                     :pr Quora/ALL))))))))

(defn lww-riak-app
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

(defn crdt-riak-app
  [opts]
  (riak-app (merge {:read {:r 1
                           :pr 1}
                    :write {:w 1
                            :dw 0
                            :pw 1}
                    :bucket-opts {:allow-siblings true
                                  :n-val 3}}
                   opts)))

(defn riak-apps
  "Returns a set of apps for testing on a set of VM hosts."
  [app-fn]
  (map #(app-fn {:host %})
       ["node1.vm" "node2.vm" "node3.vm" "node4.vm" "node5.vm"]))
