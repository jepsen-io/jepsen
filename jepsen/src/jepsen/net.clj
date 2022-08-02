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
            [slingshot.slingshot :refer [throw+]]))

; TODO: move this into jepsen.net.proto
(defprotocol Net
  (drop! [net test src dest]  "Drop traffic between nodes src and dest.")
  (heal! [net test]           "End all traffic drops and restores network to fast operation.")
  (slow! [net test]
         [net test opts]
         "Delays network packets with options:

         :mean         (in ms)
         :variance     (in ms)
         :distribution (e.g. :normal)")
  (flaky! [net test]          "Introduces randomized packet loss")
  (fast!  [net test]          "Removes packet loss and delays.")
  (shape! [net test faults]   "Shapes packet traffic by applying network emulation faults."))

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

(def packet-faults
  "All of the packet faults and their default option values.

   Caveats:
   
     - faults are applied at the node network level, i.e. all egress regardless of destination, type, etc.
     - `:delay` - Use `:normal` distribution of delays for more typical network behavior. 
     - `:loss`  - When used locally (not on a bridge or router), the loss is reported to the upper level protocols.
                  This may cause TCP to resend and behave as if there was no loss.
   
   See [tc-netem(8)](https://manpages.debian.org/bullseye/iproute2/tc-netem.8)."
  {:delay     {:time         :50ms
               :jitter       :10ms
               :correlation  :25%
               :distribution [:distribution :normal]}  ; uniform | normal | pareto |  paretonormal
   :loss      {:percent      :20%
               :correlation  :75%}
   :corrupt   {:percent      :20%
               :correlation  :75%}
   :duplicate {:percent      :20%
               :correlation  :75%}
   :reorder   {:percent      :20%
               :correlation  :75%}})

(defn- delete-netem!
  "Deletes a network emulation to remove any packet faults."
  [_net test]
  (with-test-nodes test
    ;; may already be reliable, i.e. no faults
    (try
      (su (exec tc :qdisc :del :dev :eth0 :root))
      :reliable
      (catch RuntimeException e
        (if (re-find #"Error: Cannot delete qdisc with handle of zero\."
                     (.getMessage e))
          :reliable
          (throw e))))))

(defn- set-netem!
  "Sets a network emulation with given `faults` to disrupt packets.
   Shared convenience call for iptables/ipfilter."
  [net test faults]
  (if (seq faults)
    (do
      (assert (if (:reorder faults)
                (:delay faults)
                true)
              "Cannot reorder packets without a delay.")
      ;; reset node networks to reliable before introducing new fault mix
      (delete-netem! net test)
      (let [tc-opts (->> faults
                         ;; fill in all unspecified opts with default values
                         (reduce (fn [acc [fault opts]]
                                   (assoc acc fault (merge (fault packet-faults) opts)))
                                 {})
                         ;; build a tc cmd line combining all faults
                         (reduce (fn [args [fault {:keys [time jitter percent correlation distribution] :as _opts}]]
                                   (case fault
                                     :delay
                                     (concat args [:delay time jitter correlation distribution])
                                     (:loss :corrupt :duplicate :reorder)
                                     (concat args [fault percent correlation])))
                                 [:qdisc :add :dev :eth0 :root :netem]))]
        (with-test-nodes test
          (do
            (su (exec tc tc-opts))
            :shaped))))
    ;; no faults desired, delete any existing shaping
    (delete-netem! net test)))

(def noop
  "Does nothing."
  (reify Net
    (drop!  [net test src dest])
    (heal!  [net test])
    (slow!  [net test])
    (slow!  [net test opts])
    (flaky! [net test])
    (fast!  [net test])
    (shape! [net test faults])))

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

    (shape! [net test faults]
      (set-netem! net test faults))

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

    (shape! [net test faults]
      (set-netem! net test faults))))
