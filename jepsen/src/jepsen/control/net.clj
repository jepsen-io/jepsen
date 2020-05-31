(ns jepsen.control.net
  "Network control functions."
  (:refer-clojure :exclude [partition])
  (:require [clojure.string :as str]
            [jepsen.control :as c]
            [slingshot.slingshot :refer [throw+]]))

(defn reachable?
  "Can the current node ping the given node?"
  [node]
  (try (c/exec :ping :-w 1 node) true
       (catch RuntimeException _ false)))

(defn local-ip
  "The local node's eth0 address"
  []
  (nth (->> (c/exec :ifconfig "eth0")
            ; Old ifconfig put addr: in front of the address; new ifconfig
            ; doesn't.
            (re-find #"inet (addr:)?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"))
       2))

(defn ip*
  "Look up an ip for a hostname. Unmemoized."
  [host]
  ; getent output is of the form:
  ; 74.125.239.39   STREAM host.com
  ; 74.125.239.39   DGRAM
  ; ...
  (let [res (c/exec :getent :ahosts host)
        ip (first (str/split (->> res
                                  (str/split-lines)
                                  (first))
                             #"\s+"))]
    (when (str/blank? ip)
      ; We get this occasionally for reasons I don't understand
      (throw+ {:type    :blank-getent-ip
               :output  res
               :host    host}))
    ip))

(def ip
  "Look up an ip for a hostname. Memoized."
  (memoize ip*))
