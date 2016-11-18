(ns jepsen.nemesis
  (:use clojure.tools.logging)
  (:require [clojure.set        :as set]
            [jepsen.util        :as util]
            [jepsen.client      :as client]
            [jepsen.control     :as c]
            [jepsen.net         :as net]))

(def noop
  "Does nothing."
  (reify client/Client
    (setup! [this test node] this)
    (invoke! [this test op] op)
    (teardown! [this test] this)))

(defn snub-nodes!
  "Drops all packets from the given nodes."
  [test dest sources]
  (->> sources (pmap #(net/drop! (:net test) test % dest)) dorun))

(defn partition!
  "Takes a *grudge*: a map of nodes to the collection of nodes they should
  reject messages from, and makes the appropriate changes. Does not heal the
  network first, so repeated calls to partition! are cumulative right now."
  [test grudge]
  (c/on-nodes test (fn snub [test node]
                     (snub-nodes! test node (get grudge node)))))

(defn bisect
  "Given a sequence, cuts it in half; smaller half first."
  [coll]
  (split-at (Math/floor (/ (count coll) 2)) coll))

(defn split-one
  "Split one node off from the rest"
  ([coll]
   (split-one (rand-nth coll) coll))
  ([loner coll]
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
      (net/heal! (:net test) test)
      this)

    (invoke! [this test op]
      (case (:f op)
        :start (let [grudge (grudge (:nodes test))]
                 (partition! test grudge)
                 (assoc op :value (str "Cut off " (pr-str grudge))))
        :stop  (do (net/heal! (:net test) test)
                   (assoc op :value "fully connected"))))

    (teardown! [this test]
      (net/heal! (:net test) test))))

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

(defn compose
  "Takes a map of fs to nemeses and returns a single nemesis which, depending
  on (:f op), routes to the appropriate child nemesis. `fs` should be a
  function which takes (:f op) and returns either nil, if that nemesis should
  not handle that :f, or a new :f, which replaces the op's :f, and the
  resulting op is passed to the given nemesis. For instance:

      (compose {#{:start :stop} (partition-random-halves)
                #{:kill}        (process-killer)})

  This routes `:kill` ops to process killer, and :start/:stop to the
  partitioner. What if we had two partitioners which *both* take :start/:stop?

      (compose {{:split-start :start
                 :split-stop  :stop} (partition-random-halves)
                {:ring-start  :start
                 :ring-stop2  :stop} (partition-majorities-ring)})

  We turn :split-start into :start, and pass that op to
  partition-random-halves."
  [nemeses]
  (assert (map? nemeses))
  (reify client/Client
    (setup! [this test node]
      (compose (util/map-vals #(client/setup! % test node) nemeses)))

    (invoke! [this test op]
      (let [f (:f op)]
        (loop [nemeses nemeses]
          (if-not (seq nemeses)
            (throw (IllegalArgumentException.
                     (str "no nemesis can handle " (:f op))))
            (let [[fs nemesis] (first nemeses)]
              (if-let [f' (fs f)]
                (assoc (client/invoke! nemesis test (assoc op :f f')) :f f)
                (recur (next nemeses))))))))

    (teardown! [this test]
      (util/map-vals #(client/teardown! % test) nemeses))))

(defn set-time!
  "Set the local node time in POSIX seconds."
  [t]
  (c/su (c/exec :date "+%s" :-s (str \@ (long t)))))

(defn clock-scrambler
  "Randomizes the system clock of all nodes within a dt-second window."
  [dt]
  (reify client/Client
    (setup! [this test _]
      this)

    (invoke! [this test op]
      (assoc op :value
             (c/with-test-nodes test
               (set-time! (+ (/ (System/currentTimeMillis) 1000)
                             (- (rand-int (* 2 dt)) dt))))))

    (teardown! [this test]
      (c/with-test-nodes test
        (set-time! (/ (System/currentTimeMillis) 1000))))))

(defn node-start-stopper
  "Takes a targeting function which, given a list of nodes, returns a single
  node or collection of nodes to affect, and two functions `(start! test node)`
  invoked on nemesis start, and `(stop! test node)` invoked on nemesis stop.
  Returns a nemesis which responds to :start and :stop by running the start!
  and stop! fns on each of the given nodes. During `start!` and `stop!`, binds
  the `jepsen.control` session to the given node, so you can just call `(c/exec
  ...)`.

  Re-selects a fresh node (or nodes) for each start--if targeter returns nil,
  skips the start. The return values from the start and stop fns will become
  the :values of the returned :info operations from the nemesis, e.g.:

      {:value {:n1 [:killed \"java\"]}}"
  [targeter start! stop!]
  (let [nodes (atom nil)]
    (reify client/Client
      (setup! [this test _] this)

      (invoke! [this test op]
        (locking nodes
          (assoc op :type :info, :value
                 (case (:f op)
                   :start (if-let [ns (-> test :nodes targeter util/coll)]
                            (if (compare-and-set! nodes nil ns)
                              (c/on-many ns (start! test c/*host*))
                              (str "nemesis already disrupting "
                                   (pr-str @nodes)))
                            :no-target)
                   :stop (if-let [ns @nodes]
                           (let [value (c/on-many ns (stop! test c/*host*))]
                             (reset! nodes nil)
                             value)
                           :not-started)))))

      (teardown! [this test]))))

(defn hammer-time
  "Responds to `{:f :start}` by pausing the given process name on a given node
  or nodes using SIGSTOP, and when `{:f :stop}` arrives, resumes it with
  SIGCONT.  Picks the node(s) to pause using `(targeter list-of-nodes)`, which
  defaults to `rand-nth`. Targeter may return either a single node or a
  collection of nodes."
  ([process] (hammer-time rand-nth process))
  ([targeter process]
   (node-start-stopper targeter
                       (fn start [t n]
                         (c/su (c/exec :killall :-s "STOP" process))
                         [:paused process])
                       (fn stop [t n]
                         (c/su (c/exec :killall :-s "CONT" process))
                         [:resumed process]))))
