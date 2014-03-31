(ns jepsen.nemesis
  (:use clojure.tools.logging)
  (:require [jepsen.client      :as client]
            [jepsen.control     :as c]
            [jepsen.control.net :as net]))

(defn bisect
  "Given a sequence, cuts it in half; smaller half first."
  [coll]
  (split-at (Math/floor (/ (count coll) 2)) coll))

(defn snub-node!
  "Drops all packets from node."
  [node]
  (c/su (c/exec :iptables :-A :INPUT :-s (net/ip node) :-j :DROP)))

(defn partition!
  "Given a collection of collections of nodes, isolates each collection of
  nodes from the others."
  [partitions]
  (let [universe (set (concat partitions))]
    (->> partitions
         (pmap (fn [component]
                 (c/on-many component
                            (->> universe
                                 (remove (set component))
                                 (map snub-node!)
                                 dorun))))
         dorun)))

(defn simple-partition
  "Responds to a :start operation by cutting the network into two halves, and
  a :stop operation by repairing the network."
  []
  (reify client/Client
    (setup! [this test _]
      (c/on-many (:nodes test) (net/heal))
      this)

    (invoke! [this test op]
      (info :nemesis op)
      (case (:f op)
        :start (partition! (bisect (:nodes test)))
        :stop  (c/on-many (:nodes test) (net/heal)))

      (assoc op :type :info :value "complete"))

    (teardown! [this test]
      (c/on-many (:nodes test) (net/heal)))))
