(ns jepsen.control.net
  "Network control functions."
  (:refer-clojure :exclude [partition])
  (:use jepsen.control))

(defn slow
  "Slows down the network."
  []
  (exec :tc :qdisc :add :dev :eth0 :root :netem :delay :50ms :10ms :distribution :normal))

(defn flaky
  "Drops packets."
  []
  (exec :tc :qdisc :add :dev :eth0 :root :netem :loss "20%" "75%"))

(defn fast
  "Fixes network."
  []
  (exec :tc :qdisc :del :dev :eth0 :root))

(defn ip
  "Look up an ip for a hostname"
  [host]
  (exec :dig :+short host))

(defn partition
  "Partitions the network."
  []
  (let [nodes (map ip [:n3 :n4 :n5])]
    (when (#{"n1" "n2"} *host*)
      (prn "partitioning from n3, n4, n5")
      (doseq [n nodes]
        (exec :iptables :-A :INPUT :-s n :-j :DROP)))
    (println (exec :iptables :--list))))

(defn heal
  "Heals a partition."
  []
  (exec :iptables :-F)
  (exec :iptables :-X)
  (println (exec :iptables :--list)))
