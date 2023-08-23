(ns jepsen.os.debian
  "Common tasks for Debian boxes."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [jepsen.util :refer [meh]]
            [jepsen.os :as os]
            [jepsen.control :as c :refer [|]]
            [jepsen.control.util :as cu]
            [jepsen.net :as net]
            [clojure.string :as str]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn setup-hostfile!
  "Makes sure the hostfile has a loopback entry for the local hostname"
  []
  (let [name    (c/exec :hostname)
        hosts   (c/exec :cat "/etc/hosts")
        hosts'  (->> hosts
                     str/split-lines
                     (map (fn [line]
                            (if (re-find #"^127\.0\.0\.1\t" line)
                              (str "127.0.0.1\tlocalhost") ; name)
                              line)))
                     (str/join "\n"))]
    (when-not (= hosts hosts')
      (c/su (c/exec :echo hosts' :> "/etc/hosts")))))

(defn time-since-last-update
  "When did we last run an apt-get update, in seconds ago"
  []
  (- (Long/parseLong (c/exec :date "+%s"))
     (Long/parseLong (c/exec :stat :-c "%Y" "/var/cache/apt/pkgcache.bin" "||" :echo 0))))

(defn update!
  "Apt-get update."
  []
  (c/su (c/exec :apt-get :--allow-releaseinfo-change :update)))

(defn maybe-update!
  "Apt-get update if we haven't done so recently."
  []
  (when (< 86400 (time-since-last-update))
    (update!)))

(defn installed
  "Given a list of debian packages (strings, symbols, keywords, etc), returns
  the set of packages which are installed, as strings."
  [pkgs]
  (let [pkgs (->> pkgs (map name) set)]
    (->> (apply c/exec :dpkg :--get-selections pkgs)
         str/split-lines
         (map (fn [line] (str/split line #"\s+")))
         (filter #(= "install" (second %)))
         (map first)
         (map (fn [p] (str/replace p #":amd64|:i386" {":amd64" "" ":i386" ""})))
         set)))

(defn uninstall!
  "Removes a package or packages."
  [pkg-or-pkgs]
  (let [pkgs (if (coll? pkg-or-pkgs) pkg-or-pkgs (list pkg-or-pkgs))
        pkgs (installed pkgs)]
    (c/su (apply c/exec :apt-get :remove :--purge :-y pkgs))))

(defn installed?
  "Are the given debian packages, or singular package, installed on the current
  system?"
  [pkg-or-pkgs]
  (let [pkgs (if (coll? pkg-or-pkgs) pkg-or-pkgs (list pkg-or-pkgs))]
    (every? (installed pkgs) (map name pkgs))))

(defn installed-version
  "Given a package name, determines the installed version of that package, or
  nil if it is not installed."
  [pkg]
  (->> (c/exec :apt-cache :policy (name pkg))
       (re-find #"Installed: ([^\s]+)")
       second))

(defn install
  "Ensure the given packages are installed. Can take a flat collection of
  packages, passed as symbols, strings, or keywords, or, alternatively, a map
  of packages to version strings. Can optionally take a collection of
  additional CLI options to be passed to apt-get."
  ([pkgs]
   (install pkgs []))
  ([pkgs apt-opts]
   (if (map? pkgs)
     ; Install specific versions
     (dorun
       (for [[pkg version] pkgs]
         (when (not= version (installed-version pkg))
           (info "Installing" pkg version)
           (c/exec :env "DEBIAN_FRONTEND=noninteractive"
                   :apt-get :install
                   :-y
                   :--allow-downgrades
                   :--allow-change-held-packages
                   apt-opts
                   (str (name pkg) "=" version)))))

     ; Install any version
     (let [pkgs    (set (map name pkgs))
           missing (set/difference pkgs (installed pkgs))]
       (when-not (empty? missing)
         (c/su
           (info "Installing" missing)
           (apply c/exec :env "DEBIAN_FRONTEND=noninteractive"
                  :apt-get :install
                  :-y
                  :--allow-downgrades
                  :--allow-change-held-packages
                  apt-opts
                  missing)))))))

(defn add-key!
  "Receives an apt key from the given keyserver."
  [keyserver key]
  (c/su
    (c/exec :apt-key :adv
            :--keyserver keyserver
            :--recv key)))

(defn add-repo!
  "Adds an apt repo (and optionally a key from the given keyserver)."
  ([repo-name apt-line]
   (add-repo! repo-name apt-line nil nil))
  ([repo-name apt-line keyserver key]
   (let [list-file (str "/etc/apt/sources.list.d/" (name repo-name) ".list")]
     (when-not (cu/exists? list-file)
       (info "setting up" repo-name "apt repo")
       (when (or keyserver key)
         (add-key! keyserver key))
       (c/exec :echo apt-line :> list-file)
       (update!)))))

(defn install-jdk11!
  "Installs an openjdk jdk11 via stretch-backports."
  []
  (c/su
    (add-repo!
      "stretch-backports"
      "deb http://deb.debian.org/debian stretch-backports main")
    (install [:openjdk-11-jdk])))

(deftype Debian []
  os/OS
  (setup! [_ test node]
    (info node "setting up debian")

    (setup-hostfile!)
    (maybe-update!)

    (c/su
      ; Packages!
      (install [:apt-transport-https
                :libzip4
                :wget
                :curl
                :vim
                :man-db
                :faketime
                :netcat-openbsd
                :ntpdate
                :unzip
                :iptables
                :psmisc
                :tar
                :bzip2
                :iputils-ping
                :iproute2
                :rsyslog
                :logrotate
                :dirmngr
                :tcpdump]))

    (meh (net/heal! (:net test) test)))

  (teardown! [_ test node]))

(def os "An implementation of the Debian OS." (Debian.))
