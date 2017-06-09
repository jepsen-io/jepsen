(ns jepsen.control.net
  "Network control functions."
  (:refer-clojure :exclude [partition])
  (:use jepsen.control)
  (:require [clojure.string :as str]))

(defn reachable?
  "Can the current node ping the given node?"
  [node]
  (try (exec :ping :-w 1 node) true
       (catch RuntimeException _ false)))

(defn local-ip
  "The local node's eth0 address"
  []
  (nth (->> (exec :ifconfig "eth0")
            (re-find #"inet addr:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"))
       1))

(defn ip
  "Look up an ip for a hostname"
  [host]
  ; getent output is of the form:
  ; 74.125.239.39   STREAM host.com
  ; 74.125.239.39   DGRAM
  ; ...
  (first (str/split (->> (exec :getent :ahosts host)
                         (str/split-lines)
                         (first))
                    #"\s+")))
