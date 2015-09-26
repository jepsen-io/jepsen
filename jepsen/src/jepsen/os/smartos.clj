(ns jepsen.os.smartos
  "Common tasks for SmartOS boxes."
  (:use clojure.tools.logging)
  (:require [clojure.set :as set]
            [jepsen.util :refer [meh]]
            [jepsen.os :as os]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
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
                            (if (and (re-find #"^127\.0\.0\.1\t" line)
                                     (not (re-find (re-pattern name) line)))
                              (str line " " name)
                              line)))
                     (str/join "\n"))]
    (c/su (c/exec :echo hosts' :> "/etc/hosts"))))

(defn time-since-last-update
  "When did we last run a pkgin update, in seconds ago"
  []
  (- (Long/parseLong (c/exec :date "+%s"))
     (Long/parseLong (c/exec :stat :-c "%Y" "/var/db/pkgin/sql.log"))))

(defn update!
  "Pkgin update."
  []
  (c/su (c/exec :pkgin :update)))

(defn maybe-update!
  "Pkgin update if we haven't done so recently."
  []
  (try (when (< 86400 (time-since-last-update))
         (update!))
       (catch Exception e
         (update!))))

(defn installed
  "Given a list of pkgin packages (strings, symbols, keywords, etc), returns
  the set of packages which are installed, as strings."
  [pkgs]
  (let [pkgs (->> pkgs (map name) set)]
    (->> (c/exec :pkgin :-p :list)
         str/split-lines
         (map (comp first #(str/split %1 #";")))
         (map (comp second (partial re-find #"(.*)-[^\-]+")))
         set
         (#(filter %1 pkgs))
         set)))

(defn uninstall!
  "Removes a package or packages."
  [pkg-or-pkgs]
  (let [pkgs (if (coll? pkg-or-pkgs) pkg-or-pkgs (list pkg-or-pkgs))
        pkgs (installed pkgs)]
    (c/su (apply c/exec :pkgin :-y :remove pkgs))))

(defn installed?
  "Are the given packages, or singular package, installed on the current
  system?"
  [pkg-or-pkgs]
  (let [pkgs (if (coll? pkg-or-pkgs) pkg-or-pkgs (list pkg-or-pkgs))]
    (every? (installed pkgs) (map name pkgs))))

(defn installed-version
  "Given a package name, determines the installed version of that package, or
  nil if it is not installed."
  [pkg]
  (some->> (c/exec :pkgin :-p :list)
           str/split-lines
           (map (comp first #(str/split %1 #";")))
           (map (partial re-find #"(.*)-[^\-]+"))
           (filter #(= (second %) (name pkg)))
           first
           first
           (re-find #".*-([^\-]+)")
           second))

(defn install
  "Ensure the given packages are installed. Can take a flat collection of
  packages, passed as symbols, strings, or keywords, or, alternatively, a map
  of packages to version strings."
  [pkgs]
  (if (map? pkgs)
                                        ; Install specific versions
    (dorun
     (for [[pkg version] pkgs]
       (when (not= version (installed-version pkg))
         (info "Installing" pkg version)
         (c/exec :pkgin :-y :install 
                 (str (name pkg) "-" version)))))

                                        ; Install any version
    (let [pkgs    (set (map name pkgs))
          missing (set/difference pkgs (installed pkgs))]
      (when-not (empty? missing)
        (c/su
         (info "Installing" missing)
         (apply c/exec :pkgin :-y :install missing))))))

(def os
  (reify os/OS
    (setup! [_ test node]
      (info node "setting up smartos")

      (setup-hostfile!)

      (maybe-update!)

      (c/su
        ; Packages!
        (install [:wget
                  :curl
                  :vim
                  :unzip
                  :rsyslog
                  :logrotate]))

      (c/su
       (c/exec :svcadm :enable :-r :ipfilter))

      (meh (net/heal)))

    (teardown! [_ test node])))
