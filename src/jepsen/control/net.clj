(ns jepsen.control.net
  "Network control functions."
  (:refer-clojure :exclude [partition])
  (:use jepsen.control))

(def Hosts-map {:n1 "n1"
              :n2 "n2"
              :n3 "n3"
              :n4 "n4"
              :n5 "n5"
              })

(def Small-partition-set #{(:n1 Hosts-map) (:n2 Hosts-map)})
(def Large-partition-set #{(:n3 Hosts-map) (:n4 Hosts-map) (:n5 Hosts-map)})

(defn slow
  "Slows down the network."
  []
  (exec :tc :qdisc :add :dev :eth0 :root :netem :delay :50ms :10ms :distribution :normal))

(defn cut-random-link
  "Cuts a random link to any of nodes."
  [nodes]
  (su (exec :iptables :-A :INPUT :-s (ip (rand-nth nodes)) :-j :DROP)))

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
  (su
    (let [nodes (map ip Large-partition-set)]
      (when (Small-partition-set *host*)
        (doseq [n nodes]
          (exec :iptables :-A :INPUT :-s n :-j :DROP))))))

(defn iptables-list
  []
  (su (exec :iptables :--list)))

(defn heal
  "Heals a partition."
  []
  (su
    (exec :iptables :-F)
    (exec :iptables :-X)))
