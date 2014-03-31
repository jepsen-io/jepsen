(ns jepsen.os.debian
  "Common tasks for Debian boxes."
  (:use clojure.tools.logging)
  (:require [jepsen.os :as os]
            [jepsen.control :as c]))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up debian")
      ; Packages!
      (c/su
        (c/exec :apt-get :install :-y
                [:wget
                 :iptables
                 :logrotate]))
      (info node "debian set up"))

    (teardown! [_ test node])))
