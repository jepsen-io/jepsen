(ns jepsen.net
  "Controls network manipulation."
  (:use jepsen.control)
  (:require [jepsen.control.net :as control.net]))

(defprotocol Net
  (drop! [net test src dest] "Drop traffic between nodes src and dest.")
  (heal! [net test]          "End all traffic drops and restores network to fast operation.")
  (slow! [net test]          "Delays network packets")
  (flaky! [net test]         "Introduces randomized packet loss")
  (fast! [net test]          "Removes packet loss and delays."))

(def noop
  "Does nothing."
  (reify Net
    (drop! [net test src dest])
    (heal! [net test])
    (slow! [net test])
    (flaky! [net test])
    (fast! [net test])))

(def iptables
  "Default iptables (assumes we control everything)."
  (reify Net
    (drop! [net test src dest]
      (on dest (su (exec :iptables :-A :INPUT :-s (control.net/ip src) :-j
                         :DROP :-w))))

    (heal! [net test]
      (on-many (:nodes test) (su
                               (exec :iptables :-F :-w)
                               (exec :iptables :-X :-w))))

    (slow! [net test]
      (on-many (:nodes test)
               (su (exec :tc :qdisc :add :dev :eth0 :root :netem :delay :50ms
                         :10ms :distribution :normal))))

    (flaky! [net test]
      (on-many (:nodes test)
               (su (exec :tc :qdisc :add :dev :eth0 :root :netem :loss "20%"
                         "75%"))))

    (fast! [net test]
      (on-many (:nodes test)
               (exec :tc :qdisc :del :dev :eth0 :root)))))

(def ipfilter
  "IPFilter rules"
  (reify Net
    (drop! [net test src dest]
      (on dest (su (exec :echo :block :in :from src :to :any | :ipf :-f :-))))

    (heal! [net test]
      (on-many (:nodes test) (su (exec :ipf :-Fa))))

    (slow! [net test]
      (on-many (:nodes test)
               (su (exec :tc :qdisc :add :dev :eth0 :root :netem :delay :50ms
                         :10ms :distribution :normal))))

    (flaky! [net test]
      (on-many (:nodes test)
               (su (exec :tc :qdisc :add :dev :eth0 :root :netem :loss "20%"
                         "75%"))))

    (fast! [net test]
      (on-many (:nodes test)
               (exec :tc :qdisc :del :dev :eth0 :root)))))
