(ns jepsen.os.debian
  "Common tasks for Debian boxes."
  (:use clojure.tools.logging)
  (:require [clojure.set :as set]
            [jepsen.util :refer [meh]]
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


(defn installed
  "Given a list of debian packages, returns the set of packages which are
  installed."
  [pkgs]
  (let [pkgs (->> pkgs (map name) set)]
    (->> (apply c/exec :dpkg :--get-selections pkgs)
         str/split-lines
         (map (fn [line] (str/split line #"\s+")))
         (filter #(= "install" (second %)))
         (map first)
         set)))

(defn installed?
  "Are the given debian packages, or singular package, installed on the current
  system?"
  [pkg-or-pkgs]
  (let [pkgs (if (coll? pkg-or-pkgs) pkg-or-pkgs (list pkg-or-pkgs))]
    (= (set pkgs) (installed pkgs))))

(defn install
  "Ensure the given packages are installed."
  [pkgs]
  (let [pkgs    (set (map name pkgs))
        missing (set/difference pkgs (installed pkgs))]
    (when-not (empty? missing)
      (c/su
        (apply c/exec :apt-get :install :-y missing)))))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up debian")

      (setup-hostfile!)

      (c/su
        ; Packages!
        (install [:wget
                  :curl
                  :unzip
                  :iptables
                  :logrotate]))

      (meh (net/heal))

      (info node "debian set up"))

    (teardown! [_ test node])))
