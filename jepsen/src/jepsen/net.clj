(ns jepsen.net
  "Controls network manipulation.

  TODO: break this up into jepsen.net.proto (polymorphism) and jepsen.net
  (wrapper fns, default args, etc)"
  (:require [dom-top.core :refer [real-pmap]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.control :refer :all]
            [jepsen.control.net :as control.net]
            [jepsen.net.proto :as p]
            [slingshot.slingshot :refer [throw+ try+]]))

; TODO: move this into jepsen.net.proto
(defprotocol Net
  (drop! [net test src dest]   "Drop traffic between nodes src and dest.")
  (heal! [net test]            "End all traffic drops and restores network to fast operation.")
  (slow! [net test]
         [net test opts]       "Delays network packets with options:
                                ```clj
                                {:mean          ; (in ms)
                                 :variance      ; (in ms)
                                 :distribution} ; (e.g. :normal)
                                ```")
  (flaky! [net test]           "Introduces randomized packet loss")
  (fast!  [net test]           "Removes packet loss and delays.")
  (shape! [net test nodes behavior] "Shapes network behavior,
                                     i.e. packet delay, loss, corruption, duplication, reordering, and rate
                                     for the given nodes."))

; Top-level API functions
(defn drop-all!
  "Takes a test and a grudge: a map of nodes to collections of nodes they
  should drop messages from, and makes those changes to the test's network."
  [test grudge]
  (let [net (:net test)]
    (if (satisfies? p/PartitionAll net)
      ; Fast path
      (p/drop-all! net test grudge)

      ; Fallback
      (->> grudge
           ; We'll expand {dst [src1 src2]} into ((src1 dst) (src2 dst) ...)
           (mapcat (fn expand [[dst srcs]]
                     (map list srcs (repeat dst))))
           (real-pmap (partial apply drop! net test))
           dorun))))

(def tc "/sbin/tc")

(defn net-dev
  "Returns the network interface of the current host."
  []
  (let [choices (su (exec :ip :-o :link :show))
        iface (->> choices
                   (str/split-lines)
                   (map (fn [ln] (let [[_match iface] (re-find #"\d+: ([^:@]+).+" ln)] iface)))
                   (remove #(= "lo" %))
                   (first))]
    (assert iface
            (str "Couldn't determine network interface!\n" choices))
    iface))

(defn qdisc-del
  "Deletes root qdisc for given dev on current node."
  [dev]
  (try+
   (su (exec tc :qdisc :del :dev dev :root))
   (catch [:exit 2] _
     ; no qdisc to del
     nil)))

(def all-packet-behaviors
  "All of the available network packet behaviors, and their default option values.

   Caveats:
   
     - behaviors are applied to a node's network interface and effect all db to db node traffic
     - `:delay` - Use `:normal` distribution of delays for more typical network behavior
     - `:loss`  - When used locally (not on a bridge or router), the loss is reported to the upper level protocols
                  This may cause TCP to resend and behave as if there was no loss.
   
   See [tc-netem(8)](https://manpages.debian.org/bullseye/iproute2/tc-netem.8)."
  {:delay     {:time         :50ms
               :jitter       :10ms
               :correlation  :25%
               :distribution :normal}
   :loss      {:percent      :20%
               :correlation  :75%}
   :corrupt   {:percent      :20%
               :correlation  :75%}
   :duplicate {:percent      :20%
               :correlation  :75%}
   :reorder   {:percent      :20%
               :correlation  :75%}
   :rate      {:rate         :1mbit}})

(defn- behaviors->netem
  "Given a map of behaviors, returns a sequence of netem options."
  [behaviors]
  (->>
   ; :reorder requires :delay
   (if (and (:reorder behaviors)
            (not (:delay behaviors)))
     (assoc behaviors :delay (:delay all-packet-behaviors))
     behaviors)
   ; fill in all unspecified opts with default values
   (reduce (fn [acc [behavior opts]]
             (assoc acc behavior (merge (behavior all-packet-behaviors) opts)))
           {})
   ; build a tc cmd line combining all behaviors
   (reduce (fn [args [behavior {:keys [time jitter percent correlation distribution rate] :as _opts}]]
             (case behavior
               :delay
               (concat args [:delay time jitter correlation :distribution distribution])
               (:loss :corrupt :duplicate :reorder)
               (concat args [behavior percent correlation])
               :rate
               (concat args [:rate rate])))
           [])))

(defn- net-shape!
  "Shared convenience call for iptables/ipfilter.
   Shape the network with tc qdisc, netem, and filter(s) so target nodes have given behavior."
  [_net test targets behavior]
  (let [results (on-nodes test
                          (fn [test node]
                            (let [nodes   (set (:nodes test))
                                  targets (set targets)
                                  targets (if (contains? targets node)
                                            (disj nodes node)
                                            targets)
                                  dev     (net-dev)]
                              ; start with no qdisc
                              (qdisc-del dev)
                              (if (and (seq targets)
                                       (seq behavior))
                                ; node will need a prio qdisc, netem qdisc, and a filter per target
                                (do
                                  (su
                                   ; root prio qdisc, bands 1:1-3 are system default prio
                                   (exec tc
                                         :qdisc :add :dev dev
                                         :root :handle "1:"
                                         :prio :bands 4 :priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1)
                                   ; band 1:4 is a netem qdisc for the behavior
                                   (exec tc
                                         :qdisc :add :dev dev
                                         :parent "1:4" :handle "40:"
                                         :netem (behaviors->netem behavior))
                                   ; filter dst ip's to netem qdisc with behavior
                                   (doseq [target targets]
                                     (exec tc
                                           :filter :add :dev dev
                                           :parent "1:0"
                                           :protocol :ip :prio :3 :u32 :match :ip :dst (control.net/ip target)
                                           :flowid "1:4")))
                                  targets)
                                ; no targets and/or behavior, so no qdisc/netem/filters
                                nil))))]
    ; return a more readable value
    (if (and (seq targets) (seq behavior))
      [:shaped   results :netem (vec (behaviors->netem behavior))]
      [:reliable results])))

(def noop
  "Does nothing."
  (reify Net
    (drop!  [net test src dest])
    (heal!  [net test])
    (slow!  [net test])
    (slow!  [net test opts])
    (flaky! [net test])
    (fast!  [net test])
    (shape! [net test nodes behavior])))

(def iptables
  "Default iptables (assumes we control everything)."
  (reify Net
    (drop! [net test src dest]
      (on dest (su (exec :iptables :-A :INPUT :-s (control.net/ip src) :-j
                         :DROP :-w))))

    (heal! [net test]
      (with-test-nodes test
        (su
          (exec :iptables :-F :-w)
          (exec :iptables :-X :-w))))

    (slow! [net test]
      (with-test-nodes test
        (su (exec tc :qdisc :add :dev :eth0 :root :netem :delay :50ms
                  :10ms :distribution :normal))))

    (slow! [net test {:keys [mean variance distribution]
                      :or   {mean         50
                             variance     10
                             distribution :normal}}]
      (with-test-nodes test
        (su (exec tc :qdisc :add :dev :eth0 :root :netem :delay
                  (str mean "ms")
                  (str variance "ms")
                  :distribution distribution))))

    (flaky! [net test]
      (with-test-nodes test
        (su (exec tc :qdisc :add :dev :eth0 :root :netem :loss "20%"
                  "75%"))))

    (fast! [net test]
      (with-test-nodes test
        (try
          (su (exec tc :qdisc :del :dev :eth0 :root))
          (catch RuntimeException e
            (if (re-find #"RTNETLINK answers: No such file or directory"
                         (.getMessage e))
              nil
              (throw e))))))

    (shape! [net test nodes behavior]
      (net-shape! net test nodes behavior))

    p/PartitionAll
    (drop-all! [net test grudge]
      (on-nodes test
                (keys grudge)
                (fn snub [_ node]
                  (when (seq (get grudge node))
                    (su (exec :iptables :-A :INPUT :-s
                              (->> (get grudge node)
                                   (map control.net/ip)
                                   (str/join ","))
                              :-j :DROP :-w))))))))

(def ipfilter
  "IPFilter rules"
  (reify Net
    (drop! [net test src dest]
      (on dest (su (exec :echo :block :in :from src :to :any | :ipf :-f :-))))

    (heal! [net test]
      (with-test-nodes test
        (su (exec :ipf :-Fa))))

    (slow! [net test]
      (with-test-nodes test
        (su (exec :tc :qdisc :add :dev :eth0 :root :netem :delay :50ms
                  :10ms :distribution :normal))))

    (slow! [net test {:keys [mean variance distribution]
                      :or   {mean         50
                             variance     10
                             distribution :normal}}]
      (with-test-nodes test
        (su (exec tc :qdisc :add :dev :eth0 :root :netem :delay
                  (str mean "ms")
                  (str variance "ms")
                  :distribution distribution))))

    (flaky! [net test]
      (with-test-nodes test
        (su (exec :tc :qdisc :add :dev :eth0 :root :netem :loss "20%"
                  "75%"))))

    (fast! [net test]
      (with-test-nodes test
        (su (exec :tc :qdisc :del :dev :eth0 :root))))

    (shape! [net test nodes behavior]
      (net-shape! net test nodes behavior))))
