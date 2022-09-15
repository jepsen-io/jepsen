(ns jepsen.os.ubuntu
  "Common tasks for Ubuntu boxes. Tested against Ubuntu 18.04."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [jepsen.util :refer [meh]]
            [jepsen.os :as os]
            [jepsen.control :as c :refer [|]]
            [jepsen.control.util :as cu]
            [jepsen.net :as net]
            [jepsen.os.debian :as debian]
            [clojure.string :as str]))

(deftype Ubuntu []
  os/OS
  (setup! [_ test node]
    (info node "setting up ubuntu")

    (debian/setup-hostfile!)

    (debian/maybe-update!)

    (c/su
      ; Packages!
      (debian/install [:apt-transport-https
                       :wget
                       :curl
                       :vim
                       :man-db
                       :faketime
                       :ntpdate
                       :unzip
                       :iptables
                       :psmisc
                       :tar
                       :bzip2
                       :iputils-ping
                       :iproute2
                       :rsyslog
                       :sudo
                       :logrotate]))

    (meh (net/heal! (:net test) test)))

  (teardown! [_ test node]))

(def os "An implementation of the Ubuntu OS." (Ubuntu.))
