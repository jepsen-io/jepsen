(ns jepsen.os.centos
  "Common tasks for CentOS boxes."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [jepsen.util :as u]
            [jepsen.os :as os]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.net :as net]
            [clojure.string :as str]))

(defn setup-hostfile!
  "Makes sure the hostfile has a loopback entry for the local hostname"
  []
  (let [name    (c/exec :hostname)
        hosts   (c/exec :cat "/etc/hosts")
        hosts'  (->> hosts
                     str/split-lines
                     (map (fn [line]
                            (if (and (re-find #"^127\.0\.0\.1" line)
                                     (not (re-find (re-pattern name) line)))
                              (str line " " name)
                              line)))
                     (str/join "\n"))]
    (c/su (c/exec :echo hosts' :> "/etc/hosts"))))

(defn time-since-last-update
  "When did we last run a yum update, in seconds ago"
  []
  (- (Long/parseLong (c/exec :date "+%s"))
     (Long/parseLong (c/exec :stat :-c "%Y" "/var/log/yum.log"))))

(defn update!
  "Yum update."
  []
  (c/su (c/exec :yum :-y :update)))

(defn maybe-update!
  "Yum update if we haven't done so recently."
  []
  (try (when (< 86400 (time-since-last-update))
         (update!))
       (catch Exception e
         (update!))))

(defn installed
  "Given a list of centos packages (strings, symbols, keywords, etc), returns
  the set of packages which are installed, as strings."
  [pkgs]
  (let [pkgs (->> pkgs (map name) set)]
    (->> (c/exec :yum :list :installed)
         str/split-lines
         (map (comp first #(str/split %1 #"\s+")))
         (map (comp second (partial re-find #"(.*)\.[^\-]+")))
         set
         ((partial clojure.set/intersection pkgs))
         u/spy)))

(defn uninstall!
  "Removes a package or packages."
  [pkg-or-pkgs]
  (let [pkgs (if (coll? pkg-or-pkgs) pkg-or-pkgs (list pkg-or-pkgs))
        pkgs (installed pkgs)]
    (info "Uninstalling" pkgs)
    (c/su (apply c/exec :yum :-y :remove pkgs))))

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
  (some->> (c/exec :yum :list :installed)
           str/split-lines
           (map (comp first #(str/split %1 #";")))
           (map (partial re-find #"(.*).[^\-]+"))
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
         (c/exec :yum :-y :install
                 (str (name pkg) "-" version)))))

    ; Install any version
    (let [pkgs    (set (map name pkgs))
          missing (set/difference pkgs (installed pkgs))]
      (when-not (empty? missing)
        (c/su
          (info "Installing" missing)
          (apply c/exec :yum :-y :install missing))))))

(defn install-start-stop-daemon!
  "Installs start-stop-daemon on centos"
  []
  (info "Installing start-stop-daemon")
  (c/su
    (c/exec :wget "http://ftp.de.debian.org/debian/pool/main/d/dpkg/dpkg_1.17.27.tar.xz")
    (c/exec :tar :-xf :dpkg_1.17.27.tar.xz)
    (c/exec "bash" "-c" "cd dpkg-1.17.27 && ./configure")
    (c/exec "bash" "-c" "cd dpkg-1.17.27 && make")
    (c/exec "bash" "-c" "cp /dpkg-1.17.27/utils/start-stop-daemon /usr/bin/start-stop-daemon")
    (c/exec "bash" "-c" "rm -f dpkg_1.17.27.tar.xz")))

(defn installed-start-stop-daemon?
  "Is start-stop-daemon Installed?"
  []
  (->> (c/exec :ls "/usr/bin")
       str/split-lines
       (some #(if (re-find #"start-stop-daemon" %) true))))

(deftype CentOS []
  os/OS
  (setup! [_ test node]
    (info node "setting up centos")

    (setup-hostfile!)

    (maybe-update!)

    (c/su
      ; Packages!
      (install [:wget
                :gcc
                :gcc-c++
                :curl
                :vim-common
                :unzip
                :rsyslog
                :iptables
                :ncurses-devel
                :iproute
                :logrotate]))

    (if (not= true (installed-start-stop-daemon?)) (install-start-stop-daemon!) (info "start-stop-daemon already installed"))

    (u/meh (net/heal! (:net test) test)))

  (teardown! [_ test node]))

(def os "Support for CentOS." (CentOS.))
