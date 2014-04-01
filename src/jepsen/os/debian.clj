(ns jepsen.os.debian
  "Common tasks for Debian boxes."
  (:use clojure.tools.logging)
  (:require [jepsen.os :as os]
            [jepsen.control :as c]))

(comment
(defn setup-hostfile!
  "Makes sure the hostfile has an entry for the local hostname"
  []
  (let [name  (c/exec :hostname)
        hosts (c/exec :cat "/etc/hosts")])))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up debian")
      (c/su
        ; Packages!
        (c/exec :apt-get :install :-y
                [:wget
                 :iptables
                 :logrotate]))
      (info node "debian set up"))

    (teardown! [_ test node])))
