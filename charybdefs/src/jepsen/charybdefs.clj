(ns jepsen.charybdefs
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn install-thrift!
  "Install thrift (compiler, c++, and python libraries) from source.

  Ubuntu includes thrift-compiler and python-thrift packages, but
  not the c++ library, so we must build it from source. We can't mix
  and match versions, so we must install everything from source."
  []
  (when-not (cu/exists? "/usr/bin/thrift")
    (c/su
     (debian/install [:automake
                      :bison
                      :flex
                      :g++
                      :git
                      :libboost-all-dev
                      :libevent-dev
                      :libssl-dev
                      :libtool
                      :make
                      :pkg-config
                      :python-setuptools
                      :libglib2.0-dev])
     (info "Building thrift (this takes several minutes)")
     (let [thrift-dir "/opt/thrift"]
       (cu/install-archive! "http://www-eu.apache.org/dist/thrift/0.10.0/thrift-0.10.0.tar.gz" thrift-dir)
       (c/cd thrift-dir
             ;; charybdefs needs this in /usr/bin
             (c/exec "./configure" "--prefix=/usr")
             (c/exec :make :-j4)
             (c/exec :make :install))
       (c/cd (str thrift-dir "/lib/py")
             (c/exec :python "setup.py" :install))))))

(defn install!
  "Ensure CharybdeFS is installed and the filesystem mounted at /faulty."
  []
  (install-thrift!)
  (let [charybdefs-dir "/opt/charybdefs"
        charybdefs-bin (str charybdefs-dir "/charybdefs")]
    (when-not (cu/exists? charybdefs-bin)
      (c/su
       (debian/install [:build-essential
                        :cmake
                        :libfuse-dev
                        :fuse]))
      (c/su
       (c/exec :mkdir :-p charybdefs-dir)
       (c/exec :chmod "777" charybdefs-dir))
      (c/exec :git :clone :--depth 1 "https://github.com/scylladb/charybdefs.git" charybdefs-dir)
      (c/cd charybdefs-dir
            (c/exec :thrift :-r :--gen :cpp :server.thrift)
            (c/exec :cmake :CMakeLists.txt)
            (c/exec :make)))
    (c/su
     (c/exec :modprobe :fuse)
     (c/exec :umount "/faulty" "||" "/bin/true")
     (c/exec :mkdir :-p "/real" "/faulty")
     (c/exec charybdefs-bin "/faulty" "-oallow_other,modules=subdir,subdir=/real")
     (c/exec :chmod "777" "/real" "/faulty"))))

(defn- cookbook-command
  [flag]
  (c/cd "/opt/charybdefs/cookbook"
        (c/exec "./recipes" flag)))

(defn break-all
  "All operations fail with EIO."
  []
  (cookbook-command "--io-error"))

(defn break-one-percent
  "1% of disk operations fail."
  []
  (cookbook-command "--probability"))

(defn clear
  "Clear a previous failure injection."
  []
  (cookbook-command "--clear"))
