(ns jepsen.nemesis
  (:use clojure.tools.logging)
  (:require [clojure.set        :as set]
            [jepsen.util        :as util]
            [jepsen.client      :as client]
            [jepsen.control     :as c]
            [jepsen.control.net :as net]))

(defn snub-node!
  "Drops all packets from node."
  [node]
  (c/su (c/exec :iptables :-A :INPUT :-s (net/ip node) :-j :DROP)))

(defn snub-nodes!
  "Drops all packets from the given nodes."
  [nodes]
  (dorun (map snub-node! nodes)))

(defn partition!
  "Takes a *grudge*: a map of nodes to the collection of nodes they should
  reject messages from, and makes the appropriate changes. Does not heal the
  network first, so repeated calls to partition! are cumulative right now."
  [grudge]
  (->> grudge
       (map (fn [[node frenemies]]
              (future
                (c/on node (snub-nodes! frenemies)))))
       doall
       (map deref)
       dorun))

(defn bisect
  "Given a sequence, cuts it in half; smaller half first."
  [coll]
  (split-at (Math/floor (/ (count coll) 2)) coll))

(defn split-one
  "Split one node off from the rest"
  [coll]
  (let [loner (rand-nth coll)]
    [[loner] (remove (fn [x] (= x loner)) coll)]))

(defn complete-grudge
  "Takes a collection of components (collections of nodes), and computes a
  grudge such that no node can talk to any nodes outside its partition."
  [components]
  (let [components (map set components)
        universe   (apply set/union components)]
    (reduce (fn [grudge component]
              (reduce (fn [grudge node]
                        (assoc grudge node (set/difference universe component)))
                      grudge
                      component))
            {}
            components)))

(defn bridge
  "A grudge which cuts the network in half, but preserves a node in the middle
  which has uninterrupted bidirectional connectivity to both components."
  [nodes]
  (let [components (bisect nodes)
        bridge     (first (second components))]
    (-> components
        complete-grudge
        ; Bridge won't snub anyone
        (dissoc bridge)
        ; Nobody hates the bridge
        (->> (util/map-vals #(disj % bridge))))))

(defn partitioner
  "Responds to a :start operation by cutting network links as defined by
  (grudge nodes), and responds to :stop by healing the network."
  [grudge]
  (reify client/Client
    (setup! [this test _]
      (c/on-many (:nodes test) (net/heal))
      this)

    (invoke! [this test op]
      (case (:f op)
        :start (let [grudge (grudge (:nodes test))]
                 (partition! grudge)
                 (assoc op :value (str "Cut off " (pr-str grudge))))
        :stop  (do (c/on-many (:nodes test) (net/heal))
                   (assoc op :value "fully connected"))))

    (teardown! [this test]
      (c/on-many (:nodes test) (net/heal)))))

(defn partition-halves
  "Responds to a :start operation by cutting the network into two halves--first
  nodes together and in the smaller half--and a :stop operation by repairing
  the network."
  []
  (partitioner (comp complete-grudge bisect)))

(defn partition-random-halves
  "Cuts the network into randomly chosen halves."
  []
  (partitioner (comp complete-grudge bisect shuffle)))

(defn partition-random-node
  "Isolates a single node from the rest of the network."
  []
  (partitioner (comp complete-grudge split-one)))

(defn majorities-ring
  "A grudge in which every node can see a majority, but no node sees the *same*
  majority as any other."
  [nodes]
  (let [U (set nodes)
        n (count nodes)
        m (util/majority n)]
    (->> nodes
         shuffle                ; randomize
         cycle                  ; form a ring
         (partition m 1)        ; construct majorities
         (take n)               ; one per node
         (map (fn [majority]    ; invert into connections to *drop*
                [(first majority) (set/difference U (set majority))]))
         (into {}))))

(defn partition-majorities-ring
  "Every node can see a majority, but no node sees the *same* majority as any
  other. Randomly orders nodes into a ring."
  []
  (partitioner majorities-ring))

(defn set-time!
  "Set the local node time in POSIX seconds."
  [t]
  (c/su (c/exec :date "+%s" :-s (long t))))

(defn clock-scrambler
  "Randomizes the system clock of all nodes within a dt-second window."
  [dt]
  (reify client/Client
    (setup! [this test _]
      this)

    (invoke! [this test op]
      (c/on-many (:nodes test)
                 (set-time! (+ (/ (System/currentTimeMillis) 1000)
                               (- (rand-int (* 2 dt)) dt)))))

    (teardown! [this test]
      (c/on-many (:nodes test)
                 (set-time! (/ (System/currentTimeMillis) 1000))))))
