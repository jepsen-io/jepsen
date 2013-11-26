(ns jepsen.control.net
  "Network control functions."
  (:refer-clojure :exclude [partition])
  (:use jepsen.control))

(def hosts-map {:n1 "n1"
              :n2 "n2"
              :n3 "n3"
              :n4 "n4"
              :n5 "n5"
              })

(def small-partition-set #{(:n1 hosts-map) (:n2 hosts-map)})
(def large-partition-set #{(:n3 hosts-map) (:n4 hosts-map) (:n5 hosts-map)})

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
  (exec :dig :+short :+search host))

(defn cut-random-link
  "Cuts a random link to any of nodes."
  [nodes]
  (su (exec :iptables :-A :INPUT :-s (ip (rand-nth nodes)) :-j :DROP)))


(defn partition
  "Partitions the network."
  []
  (su
    (let [nodes (map ip large-partition-set)]
      (when (small-partition-set *host*)
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
