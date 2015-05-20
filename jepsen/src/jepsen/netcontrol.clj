(ns jepsen.netcontrol
  "Controls network manipulation."
  (:use jepsen.control))

(defprotocol NetController
  (drop-from! [net source] "Drop traffic originating from this source IP.")
  (heal!      [net]        "Remove all partition-inducing rules."))

(def noop
  "Does nothing"
  (reify NetController
    (drop-from! [net source])
    (heal!      [net])))

(def iptables
  "Default iptables (assumes we control everything)."
  (reify NetController
    (drop-from! [net source]
      (su (exec :iptables :-A :INPUT :-s source :-j :DROP)))

    (heal!      [net]
      (su
        (exec :iptables :-F)
        (exec :iptables :-X)))))
