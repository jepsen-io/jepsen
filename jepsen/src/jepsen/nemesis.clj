(ns jepsen.nemesis
  (:use clojure.tools.logging)
  (:require [clojure.set        :as set]
            [jepsen.util        :as util]
            [jepsen.client      :as client]
            [jepsen.control     :as c]
            [jepsen.control.net :as net]))

(defn noop
  "Does nothing."
  []
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op] op)
    (teardown! [this test] this)))

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

(defn single-node-start-stopper
  "Takes a function (start! test node) invoked on nemesis start, and one
  invoked on nemesis stop, and a targeting function which, given a list of
  nodes, picks the next one to experience bij. Returns a nemesis which responds
  to :start and :stop by running the start! and stop! fns on a selected node.
  During start!  and stop!, binds the jepsen.control session to the given node,
  so you can just call (c/exec ...).

  Re-selects a fresh node every start--if targeter returns nil, skips the
  start. The return values from the start and stop fns will become the :value
  of the returned :info operations from the nemesis."
  [targeter start! stop!]
  (let [node (atom nil)]
    (reify client/Client
      (setup! [this test _] this)

      (invoke! [this test op]
        (locking node
          (assoc op :type :info, :value
                 (case (:f op)
                   :start (if-let [n (targeter (:nodes test))]
                            (if (compare-and-set! node nil n)
                              (c/on n (start! test n))
                              (str "nemesis already disrupting " @node))
                            :no-target)
                   :stop (if-let [n @node]
                           (let [value (c/on n (stop! test n))]
                             (reset! node nil)
                             value)
                           :not-started)))))

      (teardown! [this test]))))

(defn hammer-time
  "Responds to `{:f :start}` by pausing the given process on a given node using
  SIGSTOP, and when `{:f :stop}` arrives, resumes it with SIGCONT. Picks the
  node to pause using `(targeter list-of-nodes)`, which defaults to
  `rand-nth`."
  ([process] (hammer-time rand-nth process))
  ([targeter process]
   (single-node-start-stopper targeter
                              (fn start [t n]
                                (c/su (c/exec :killall :-s "STOP" process))
                                [n process :paused])
                              (fn stop [t n]
                                (c/su (c/exec :killall :-s "CONT" process))
                                [n process :resumed]))))
