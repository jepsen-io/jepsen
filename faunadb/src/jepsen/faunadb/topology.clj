(ns jepsen.faunadb.topology
  "Working with FaunaDB topologies: the assignment of nodes to replicas, and
  log nodes."
  (:require [clojure.set :as set]
            [jepsen.util :refer [rand-nth-empty]]))

(defn replica-name
  "Constructs a replica name for a given replica number."
  [n]
  (str "replica-" n))

(defn initial-topology
  "Constructs an initial topology for a test. Test must have :nodes, which is a
  collection of all nodes in the cluster, and :replicas, an initial replica
  count. Constructs a topology: a map of:

      {:replica-count number-of-replicas
       :nodes         [{:node     node-name,
                        :state    :active
                        :replica  replica-name}, ...]}"
  [test]
  {:replica-count (:replicas test)
   :nodes (->> (:nodes test)
               (map-indexed
                 (fn [i node]
                   {:node    node
                    :state   :active
                    :replica (replica-name (mod i (:replicas test)))})))})

; Node accessors

(defn get-node
  "Given a topology and a node name, returns the node structure for that node
  name."
  [topo node-name]
  (first (filter (fn [node] (= node-name (:node node))) (:nodes topo))))

(defn assoc-node
  "Given a topology, a node name, and a new node structure value, replaces the
  node in that topology having that name, with the given value."
  [topo node-name v]
  (assoc topo :nodes
         (map (fn [node]
                (if (= node-name (:node node))
                  v
                  node))
              (:nodes topo))))

(defn update-node
  "Given a topology, a node name, a function, and args, updates the topology by
  transforming the node by applying (f node & args)."
  [topo node-name f & args]
  (assoc-node topo node-name (apply f (get-node topo node-name) args)))

; Working with replicas

(defn replicas
  "The set of all replicas in a topology."
  [topo]
  (->> topo :replica-count range (map replica-name)))

(defn replica
  "Given a topology and a node name, returns the replica name for that node."
  [topo node]
  (:replica (get-node topo node)))

(defn nodes-by-replica
  "Given a topology, constructs a map of replica names to the node names in
  that replica."
  [topo]
  (->> topo :nodes (map :node) (group-by (partial replica topo))))

(defn only-active
  "Returns a version of a topology with only nodes which are :active."
  [topo]
  (assoc topo :nodes (filter (fn [node] (= :active (:state node)))
                             (:nodes topo))))

; Log partitions

(defn log-parts
  "All log parts in the given topology."
  [topo]
  (range (inc (reduce max 0 (keep :log-part (:nodes topo))))))

(defn smallest-log-part
  "Returns the smallest log part in the given topology."
  [topo]
  (apply min-key (frequencies (map :log-part (:nodes topo)))
         (log-parts topo)))

(defn log-configuration
  "Configuration for the transaction log for the given topology. Returns a list
  of log partitions; each partition being a list of nodes."
  [topo]
  (let [grouped (group-by :log-part (:nodes topo))]
    (->> (log-parts topo)
         (map grouped)
         (map (partial map :node)))))

; State transitions

(defn add-ops
  "Given a test and a topology, constructs a set of all the add operations that
  we could apply."
  [test topo]
  (let [active (mapv :node (:nodes topo))]
    (when (seq active)
      (map (fn [node] {:type  :info
                       :f     :add-node
                       :value {:node node
                               :join (rand-nth active)}})
           (set/difference (set (:nodes test)) (set active))))))

(defn remove-ops
  "All node remove operations we could currently execute."
  [test topo]
  (let [topo (only-active topo)
        ; You can only remove nodes which aren't participating in the log
        ; 2.6.0 and higher don't require log config
        ;without-log-part (->> topo
        ;                     :nodes
        ;                     (remove :log-part)
        ;                     (map :node)
        ;                     set)
        ; We need to make sure not to empty a replica
        with-enough-nodes-in-replica (->> (nodes-by-replica topo)
                                          vals
                                          (filter #(< 1 (count %)))
                                          (reduce concat)
                                          set)
        ; Candidates for removal
        ; 2.6.0: don't need log config
        ; candidates (set/intersection without-log-part
        ;                             with-enough-nodes-in-replica)]
        candidates with-enough-nodes-in-replica]
    (map (fn [node] {:type :info, :f :remove-node, :value node})
         candidates)))

(def min-log-part-node-count
  "What's the smallest log partition we'll tolerate? I think shrinking to 1
  might break Fauna"
  2)

(defn remove-log-node-ops
  "All possible operations for removing a node from the log topology."
  [test topo]
  (->> topo
       :nodes
       (filter :log-part)
       (group-by :log-part)
       (mapcat (fn [[part nodes]]
                 (when (< min-log-part-node-count (count nodes))
                   (map :node nodes))))
       (map (fn [node]
              {:type :info, :f :remove-log-node, :value node}))))

(defn ops
  "All operations we could execute on a given test and topology. If no topology
  is given, uses the test's current topology."
  ([test]
   (ops test @(:topology test)))
  ([test topo]
   (concat (add-ops test topo)
           (remove-log-node-ops test topo)
           (remove-ops test topo))))

(defn rand-op
  "A random op on the given test and topology. If no topology is given, uses
  the test's current topology."
  ([test]
   (ops test @(:topology test)))
  ([test topo]
   ; We do this because there might be lots of remove-log-node ops we could
   ; execute, relative to a small number of remove-node ops, and we want a more
   ; even distribution of *types* of operations.
   (->> [(add-ops test topo)
         (remove-ops test topo)]
        ; 2.6.0+ don't require manual log config
        ; (remove-log-node-ops test topo)]
        (keep rand-nth-empty)
        shuffle
        first)))

(defn apply-op
  "Given a topology and a topology transition operation like {:type :info, :f
  :add-node, {:node \"a\", :join \"b\"}}, returns the topology that would
  result from applying this operation.

  We need this because in order to remove a node, we need to distribute a new
  log topology to every node in the cluster without the target node. To compute
  that log configuration, we need a topology--the topology that *would result*
  from removing the target node, but hasn't occurred yet.

  This brings up some really fun questions, like: what happens if the topology
  gets out of sync with what the cluster actually has? What if a transition
  operation crashes, but could complete later? So... all of this stuff is
  best-effort."
  [topo op]
  (case (:f op)
    :remove-log-node (update-node topo (:value op) dissoc :log-part)
    :add-node (update topo :nodes conj
                      {:node    (:node (:value op))
                       :state   :active
                       :replica (replica-name
                                  (rand-int (:replica-count topo)))})
    :remove-node (update-node topo (:value op) assoc :state :removing)))
