(ns jepsen.net
  "Controls network manipulation.

  TODO: break this up into jepsen.net.proto (polymorphism) and jepsen.net
  (wrapper fns, default args, etc)"
  (:use jepsen.control)
  (:require [jepsen.control.net :as control.net]))

(defprotocol Net
  (drop! [net test src dest] "Drop traffic between nodes src and dest.")
  (heal! [net test]          "End all traffic drops and restores network to fast operation.")
  (slow! [net test]
         [net test opts]
         "Delays network packets with options:

         :mean         (in ms)
         :variance     (in ms)
         :distribution (e.g. :normal)")
  (flaky! [net test]         "Introduces randomized packet loss")
  (fast! [net test]          "Removes packet loss and delays."))

(def tc "/sbin/tc")

(def noop
  "Does nothing."
  (reify Net
    (drop! [net test src dest])
    (heal! [net test])
    (slow! [net test])
    (slow! [net test opts])
    (flaky! [net test])
    (fast! [net test])))

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
              (throw e))))))))

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
        (su (exec :tc :qdisc :del :dev :eth0 :root))))))
