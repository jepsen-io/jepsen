(ns jepsen.nemesis.membership
  "EXPERIMENTAL: provides standardized support for nemeses which add and remove
  nodes from a cluster.

  This is a tricky problem. Even the concept of cluster state is complicated:
  there is Jepsen's knowledge of the state, and each individual node's
  understanding of the current state. Depending on which node you ask, you may
  get more or less recent (or, frequently, divergent) views of cluster state.
  Cluster state representation is highly variable across databases, which means
  our standardized state machine must allow for that variability.

  We are guided by some principles that crop up repeatedly in writing these
  sorts of nemeses:

  1. We should avoid creating useless cluster states--e.g. those that can't
     fulfill any requests--for very long.

  2. There are both safe and unsafe transitions. In general, commands like
     join/remove should always be safe. Removing data, however, is *unsafe*
     unless we can prove the node has been properly removed.

  3. We *want* to leave nodes running, with data files intact, after removing
     them. This is when interesting things happen.

  4. We must be safe in the presence of concurrent node kill/restart
     operations.

  5. Nodes tend to go down or fail to reach the rest of the cluster, but we
     want to continue making decisions during this time.

  6. Requested changes to the cluster may time out, or simply take a while to
     perform. We need to *remember* these ongoing operations, use them to
     constrain our choices of further changes (e.g. if four node removals are
     underway, don't initiate a fifth), and find ways to resolve those ongoing
     changes, e.g. by confirming they took place.

  Our general approach is to define a sort of state machine where the state is
  our representation of the cluster state, how all nodes view the cluster, and
  the set of ongoing operations, plus any auxiliary material (e.g. after
  completing a node removal, we can delete its data files). This state is
  periodically *updated* by querying individual nodes, and *also* by performing
  operations--e.g. initiating a node removal.

  The generator constructs those operations by asking the nemesis what sorts of
  operations would be legal to perform at this time, and picking one of those.
  It then passes that operation back to the nemesis (via nemesis/invoke!), and
  the nemesis updates its local state and performs the operation."
  (:refer-clojure :exclude [resolve])
  (:require [clojure [pprint :refer [pprint]]
                     [set :as set]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as nem]
                    [util :as util :refer [fixed-point
                                           pprint-str]]]
            [jepsen.nemesis.membership.state :as state]))

(def node-view-interval
  "How many seconds between updating node views."
  5)

(def State
  "For convenience, a copy of the membership State protocol. This lets users
  implement the protocol without requiring the state namespace themselves."
  state/State)

(defn initial-state
  "Constructs an initial cluster state map for the given test."
  [test]
  (let [nodes (:nodes test)]
    {; A map of node names to their view of the cluster
     :node-views {}
     ; A merged view derived from those node views, plus any local state
     :view       nil
     ; A set of [op op] operations we have applied
     :pending    #{}}))

(defn resolve-ops
  "Try to resolve any pending ops we can. Returns state with those ops
  resolved."
  [state test opts]
  (reduce (fn [state op-pair]
            (if-let [state' (state/resolve-op state test op-pair)]
              ; Ah good, resolved.
              (do (when (:log-resolve-op? opts)
                    (info (str "Resolved pending membership operation:\n"
                             (pprint-str op-pair))))
                  (update state' :pending disj op-pair))
              ; Nope!
              state))
          state
          (:pending state)))

(defn resolve
  "Resolves a state towards its final form by calling resolve and resolve-ops
  until converged."
  [state test opts]
  (let [state' (fixed-point (fn [state]
                              (-> state
                                  (state/resolve test)
                                  (resolve-ops test opts)))
                            state)]
    (when (and (:log-resolve? opts)
               (not= state state'))
      (info (str "Membership state resolved to\n" (pprint-str state'))))
    state'))

(defn update-node-view!
  "Takes an atom wrapping a State, a test, and a node. Gets the current view
  from that node's perspective, and updates the state atom to reflect it."
  [state test node opts]
  ; Get the state from the current node, presumably via
  ; shell/network commands
  (when-let [nv (state/node-view @state test node)]
    (when (and (:log-node-views? opts)
               (not= nv (get-in @state [:node-views node])))
      (info (str "New node view from " node ":\n")
            (pprint-str nv)))
    (let [; And merge it into the state atom.
          changed-view (atom nil)
          s (locking state
              (swap! state
                     (fn merge-views [state]
                       ; Update this node view
                       (let [node-views' (assoc (:node-views state) node nv)
                             state'      (assoc state :node-views node-views')
                             ; Merge views together
                             view'       (state/merge-views state' test)
                             state'      (assoc state' :view view')
                             ; Resolve any changes
                             state'      (resolve state' test opts)]
                         ; For logging purposes, keep track of changes
                         (reset! changed-view
                                 (when (not= (:view state) view')
                                   view'))
                         state'))))]
      (when (:log-view? opts)
        (when-let [v @changed-view]
          (info (str "New membership view (from " node "):\n"
                     (pprint-str v))))))))

(defn node-view-future
  "Spawns a future which keeps the given state atom updated with our view of
  this node."
  [test state running? opts node]
  (future
    (c/on node
          (while @running?
            (try (update-node-view! state test node opts)
                 (catch InterruptedException e
                   ; This normally happens at the end of our test; changes are
                   ; good we're shutting down.
                   nil)
                 (catch Throwable t
                   (warn t "Node view updater caught throwable; will retry")))
            (Thread/sleep (* 1000 node-view-interval))))))

(defrecord Nemesis
  [; An atom that tracks our current state
   state
   ; Used to terminate futures
   running?
   ; A collection of futures we use to keep each node's view
   ; up to date.
   node-view-futures
   opts]

  nem/Nemesis
  (setup! [this test]
    ; Initialize our state
    (locking state
      (swap! state into (initial-state test))
      (swap! state state/setup! test))

    (let [; We'll use this atom to track whether to shut down.
          running? (atom true)
          ; Spawn futures to update nodes.
          futures  (mapv (partial node-view-future
                                  test
                                  state
                                  running?
                                  opts)
                         (:nodes test))]
      (assoc this
             :running?          running?
             :node-view-futures futures)))

  (invoke! [this test op]
    ; Resolve pending ops, and record our new op.
    (locking state
      (let [; Apply the operation.
            s   @state
            res (state/invoke! s test op)
            op' (if (vector? res)
                  ; If invoke! yielded a pair, we replace the current state
                  ; with the new one.
                  (let [[op' s'] res]
                    ; This shouldn't happen if we lock correctly, but let's
                    ; make sure
                    (when-not (compare-and-set! state s s')
                      (throw (IllegalStateException.
                               "Membership nemesis state changed during invoke!")))
                    op')
                  ; Invoke yielded a non-pair; assume it was an op.
                  res)]
        ; Update the map to reflect the operation.
        (swap! state (fn [state]
                       (-> state
                           (update :pending conj [op op'])
                           (resolve test opts))))
        op')))

  (teardown! [this test]
    ; Graceful shutdown
    (when running?
      (reset! running? false)
      ; Ungraceful shutdown
      (mapv future-cancel node-view-futures)
      ; Shut down state
      (state/teardown! @state test)))

  nem/Reflection
  (fs [this]
    (state/fs @state)))

(defrecord Generator [state]
  gen/Generator
  (update [this test ctx event] this)

  (op [this test ctx]
    (when-let [op (state/op @state test)]
      ; Expand that into a proper map
      [(if (= :pending op)
        :pending
        (gen/fill-in-op op ctx))
       this])))

(defn package
  "Constructs a nemesis and generator for membership operations. Options
  are a map like

    {:faults #{:membership ...}
     :membership membership-opts}.

  Membership opts are:

    {:state           A record satisfying the State protocol
     :log-resolve-op? Whether to log the resolution of operations
     :log-resolve?    Whether to log each resolve step
     :log-node-views? Whether to log changing node views
     :log-view?       Whether to log the entire cluster view.

  The package includes a :state field, which is an atom of the current cluster
  state. You can use this (for example) to have generators which inspect the
  current cluster state and use it to target faults."
  [opts]
  (when (contains? (:faults opts) :membership)
    (let [mopts    (:membership opts)
          state   (atom (:state mopts))
          nem     (map->Nemesis {:state state
                                 :opts (select-keys mopts [:log-node-views?
                                                           :log-view?
                                                           :log-resolve?
                                                           :log-resolve-op?])})
          gen     (->> (Generator. state)
                       (gen/stagger (:interval opts 10)))]
      {:state     state
       :nemesis   nem
       :generator gen})))
