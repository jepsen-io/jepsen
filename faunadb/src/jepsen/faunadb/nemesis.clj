(ns jepsen.faunadb.nemesis
  "nemesis for FaunaDB"
  (:require [jepsen
             [control :as c]
             [nemesis :as nemesis]
             [net :as net]
             [generator :as gen]
             [reconnect :as rc]
             [util :as util :refer [letr]]]
            [jepsen.nemesis.time :as nt]
            [jepsen.faunadb [auto :as auto]
                            [topology :as topo]]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;; Nemesis definitions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn single-node-partition-start
  "A generator for a network partition start operation, which isolates a single
  node from all other nodes."
  [test process]
  {:type  :info
   :f     :start-partition
   :value (->> test :nodes nemesis/split-one nemesis/complete-grudge)
   :partition-type :single-node})

(defn intra-replica-partition-start
  "A generator for a network partition start operation, which creates a
  partition inside a single replica. Nodes from other replicas have
  uninterrupted connectivity to nodes in the affected replica."
  [test process]
  (let [[replica nodes] (rand-nth (vec (topo/nodes-by-replica
                                         @(:topology test))))
        grudge (-> nodes shuffle nemesis/bisect nemesis/complete-grudge)]
    {:type :info
     :f :start-partition
     :value grudge
     :partition-type [:intra-replica replica]}))

(defn inter-replica-partition-start
  "A generator for a network partition start operation, which creates a
  partition between replicas, diving a single replica from the others."
  [test process]
  (let [grudge (->> @(:topology test)
                    topo/nodes-by-replica ; Map of replicas to groups of nodes
                    vals            ; List of groups of nodes
                    shuffle         ; List of groups of nodes
                    nemesis/bisect  ; [majority list of groups, minority]
                    (map flatten)   ; [majority nodes, minority nodes]
                    nemesis/complete-grudge)] ; Grudge
    {:type :info, :f :start-partition, :value grudge,
     :partition-type :inter-replica}))

(defn with-refresh
  "Wraps generator in a generator that refreshes the topology before generating
  an op."
  [gen]
  (reify gen/Generator
    (op [this test process]
      (auto/refresh-topology! test)
      (gen/op gen test process))))

(defn topo-op
  "A generator for a random topology transition."
  [test process]
  (let [topo @(:topology test)]
    (or (topo/rand-op test topo)
        (do (info "No topology transitions possible for\n"
                  (with-out-str (pprint topo)))
            nil))))

(defn topo-nemesis
  "Adds and removes nodes from the cluster."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [v    (:value op)
            topo @(:topology test)
            topo' (topo/apply-op topo op)
            _   (info "Target topology:\n" (with-out-str (pprint topo')))
            res (case (:f op)
                  :remove-log-node
                  (do (info "New log configuration will be"
                            (topo/log-configuration topo'))
                      (c/on-nodes test (map :node (:nodes topo))
                                  (fn [test node]
                                    (auto/configure! test topo' node)
                                    ;(locking topo'
                                      ; Stagger these so we have a chance to see
                                      ; something interesting happen. Doing them
                                      ; serially takes for evvvvver
                                      ; (Thread/sleep (rand-int 10000))
                                      (auto/stop! test node)
                                      (auto/start! test node)))
                      [:removed-log-node v])

                  :add-node (do (c/on-nodes test (map :node (:nodes topo'))
                                        (fn [test node]
                                          (auto/configure! test topo' node)
                                          (when (= node (:node v))
                                            (auto/start! test node)
                                            (auto/join! (:join v))
                                            (info :status (auto/status)))))
                                [:added v])

                  :remove-node
                  (do ; Fauna suggests that the official way of removing
                      ; nodes--doing it while the node is up and running--is
                      ; something that nobody may have tested before. We're
                      ; going to try stopping the node THEN removing it.
                      (c/on-nodes test [v] (fn [test node]
                                             (auto/kill! test node)
                                             (auto/delete-data-files!)))
                      (util/with-retry []
                        (c/on-nodes test [(->> (:nodes topo)
                                               (map :node)
                                               (remove #{v}) ; Can't remove self
                                               vec
                                               rand-nth)]
                                    (fn [test local-node]
                                      @(auto/remove-node! v)
                                      (info :status (auto/status))))
                        ; sometimes we pick a node to perform a removal
                        ; that is itself already removed, so it fails with
                        ; "Host is unknown" error message. Here we retry and
                        ; hope we pick a better host next time.
                        (catch clojure.lang.ExceptionInfo e (retry))
                      )
                      [:removed v]))]

        ; Go ahead and update the new topology
        (reset! (:topology test) topo')
        (assoc op :value res)))

    (teardown! [this test])))

(defn restart-stop-kill
  "A nemesis that starts all nodes, or stops or kills a randomly selected
  subset of nodes."
  []
  (nemesis/timeout 60000
    (reify nemesis/Nemesis
      (setup! [this test] this)

      (invoke! [this test op]
        (let [nodes (->> test :topology deref :nodes (map :node))
              nodes (case (:f op)
                      :restart nodes
                      (:stop :kill) (util/random-nonempty-subset nodes))]
          (assoc op :value
                 (c/on-nodes test nodes
                             (case (:f op)
                               :restart  auto/start!
                               :stop     auto/stop!
                               :kill     auto/kill!)))))

      (teardown! [this test]))))

(defn topology
  "A nemesis package which randomly permutes the set of nodes in the cluster."
  []
  {:clocks  false
   :nemesis (topo-nemesis)
   :during  (->> topo-op
                 with-refresh)
   :final   nil})

(defn full-nemesis
  "Merges together all failure modes into a single nemesis."
  []
  (nemesis/compose
    {#{:restart
       :kill
       :stop}                     (restart-stop-kill)
     {:start-partition     :start
      :stop-partition      :stop} (nemesis/partitioner nil)
     #{:add-node
       :remove-node}              (topo-nemesis)
     {:reset-clock         :reset
      :strobe-clock        :strobe
      :check-clock-offsets :check-offsets
      :bump-clock          :bump} (nt/clock-nemesis)}))

(defn op
  "Construct a nemesis op with the given f and no value."
  [f]
  {:type :info, :f f, :value nil})

(defn clock-gen
  "Generator of clock skew operations."
  []
  (->> [(fn [test process]
          {:type  :info
           :f     :bump
           ; Tune this number up or down to control the clock skew; -2000 runs
           ; the clock on this node two seconds behind.
           :value {(rand-nth (:nodes test)) -20000}})
        (fn [test process] {:type :info, :f :reset, :value (:nodes test)})]
       cycle
       gen/seq))

(defn full-generator
  "Takes nemesis options and constructs a generator for the nemesis operations
  specified in (:nemesis test); e.g. process kills and partitions."
  [n]
  (->> (cond-> []
         (:kill n)
         (conj (op :kill) (op :restart))

         (:stop n)
         (conj (op :stop) (op :restart))

         (:inter-replica-partition n)
         (conj inter-replica-partition-start (op :stop-partition))

         (:intra-replica-partition n)
         (conj intra-replica-partition-start (op :stop-partition))

         (:single-node-partition n)
         (conj single-node-partition-start (op :stop-partition))

         (:clock-skew n) (conj (->> (nt/clock-gen)
                                    (gen/f-map {:check-offsets :check-clock-offsets
                                                :reset  :reset-clock
                                                :strobe :strobe-clock
                                                :bump   :bump-clock})))

         (:topology n)  (conj (with-refresh topo-op)))
       gen/mix
       (gen/stagger (:interval n))))

(defn nemesis
  "Composite nemesis and generator"
  [opts]
  {:nemesis         (full-nemesis)
   :generator       (full-generator opts)
   :final-generator (->> [(when (:clock-skew opts) :reset-clock)
                          (when (or (:inter-replica-partition opts)
                                    (:intra-replica-partition opts)
                                    (:single-node-partition opts))
                            :stop-partition)
                          (when (or (:stop opts) (:kill opts)) :restart)]
                         (remove nil?)
                         (map op)
                         gen/seq)})
