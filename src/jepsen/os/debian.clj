(ns jepsen.os.debian
  "Common tasks for Debian boxes."
  (:use clojure.tools.logging)
  (:require [jepsen.util :refer [meh]]
            [jepsen.os :as os]
            [jepsen.control :as c]
            [jepsen.control.net :as net]
            [clojure.string :as str]))

(defn setup-hostfile!
  "Makes sure the hostfile has a loopback entry for the local hostname"
  []
  (let [name    (c/exec :hostname)
        hosts   (c/exec :cat "/etc/hosts")
        hosts'  (->> hosts
                     str/split-lines
                     (map (fn [line]
                            (if (re-find #"^127\.0\.0\.1\tlocalhost$" line)
                              (str "127.0.0.1\tlocalhost " name)
                              line)))
                     (str/join "\n"))]
    (when-not (= hosts hosts')
      (c/su (c/exec :echo hosts' :> "/etc/hosts")))))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up debian")

      (setup-hostfile!)

      (c/su
        ; Packages!
        (c/exec :apt-get :install :-y
                [:wget
                 :iptables
                 :logrotate]))

      (meh (net/heal))

      (info node "debian set up"))

    (teardown! [_ test node])))
