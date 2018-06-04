(ns jepsen.os.ubuntu
  "Common tasks for Ubuntu/CockroachDB boxes."
  (:use clojure.tools.logging)
  (:require [clojure.set :as set]
            [jepsen.util :refer [meh]]
            [jepsen.os :as os]
            [jepsen.os.debian :as debian]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.net :as net]
            [clojure.string :as str]))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up ubuntu")

      (debian/setup-hostfile!)

      (debian/maybe-update!)

      (c/su
       ;; Packages!
       (debian/install [:wget
                        :curl
                        :vim
                        :man-db
                        :faketime
                        :unzip
                        :ntpdate
                        :iptables
                        :iputils-ping
                        :rsyslog
                        :tcpdump
                        :logrotate])
       ;; This occasionally fails (roughly 1% of the time) for no apparent reason
       ;; (no log messages I've been able to find). Sometimes it fails
       ;; several times in a row. Keep trying until the process is
       ;; gone.
       ;;
       ;; TODO: This assumes ubuntu 16.04, which uses ntpd. Ubuntu
       ;; 18.04 switches to chronyd instead so this will need to be
       ;; updated.
       (c/su (c/exec :service :ntp :stop "||"
                     :while "!" :pgrep :ntpd (c/lit ";") :do
                     :sleep "1" (c/lit ";") :service :ntp :stop "||" :true (c/lit ";")
                     :done)))

      (meh (net/heal! (:net test) test)))

    (teardown! [_ test node])))
