(ns jepsen.system.etcd
  (:require [clojure.tools.logging  :refer [debug info warn]]
            [clojure.java.io        :as io]
            [clojure.string         :as str]
            [jepsen.core            :as core]
            [jepsen.util            :refer [meh timeout]]
            [jepsen.codec           :as codec]
            [jepsen.core            :as core]
            [jepsen.control         :as c]
            [jepsen.control.net     :as net]
            [jepsen.control.util    :as cu]
            [jepsen.client          :as client]
            [jepsen.db              :as db]
            [jepsen.generator       :as gen]
            [jepsen.os.debian       :as debian]
            [knossos.core           :as knossos]
            [cheshire.core          :as json]))

(def binary "/opt/etcd/bin/etcd")
(def pidfile "/var/run/etcd.pid")
(def data-dir "/var/lib/etcd")
(def log-file "/var/log/etcd.log")

(defn peer-addr [node]
  (str (name node) ":7001"))

(defn addr [node]
  (str (name node) ":4001"))

(defn peers
  "The command-line peer list for an etcd cluster."
  [test]
  (->> test
       :nodes
       (map peer-addr)
       (str/join ",")))

(defn running?
  "Is etcd running?"
  []
  (try
    (c/exec :start-stop-daemon :--status
            :--pidfile pidfile
            :--exec binary)
    true
    (catch RuntimeException _ false)))

(defn start-etcd!
  [test node]
  (info node "starting etcd")
  (c/exec :start-stop-daemon :--start
          :--background
          :--make-pidfile
          :--pidfile        pidfile
          :--chdir          "/opt/etcd"
          :--exec           binary
          :--no-close
          :--
          :-peer-addr       (peer-addr node)
          :-addr            (addr node)
          :-peer-bind-addr  "0.0.0.0:7001"
          :-bind-addr       "0.0.0.0:4001"
          :-data-dir        data-dir
          :-name            (name node)
          (when-not (= node (core/primary test))
            [:-peers        (peers test)])
          :>>               log-file
          (c/lit "2>&1")))

(defn db []
  (let [running (atom nil)] ; A map of nodes to whether they're running
    (reify db/DB
      (setup! [this test node]
        ; You'll need debian testing for this, cuz etcd relies on go 1.2
        (debian/install [:golang :git-core])

        (c/su
          (c/cd "/opt"
                (when-not (cu/file? "etcd")
                  (info node "cloning etcd")
                  (c/exec :git :clone "https://github.com/coreos/etcd")))

          (c/cd "/opt/etcd"
                (when-not (cu/file? "bin/etcd")
                  (info node "building etcd")
                  (c/exec (c/lit "./build"))))

          ; There's a race condition in cluster join we gotta work around by
          ; restarting the process until it doesn't crash; see
          ; https://github.com/coreos/etcd/issues/716"

          ; Initially, every node is not running.
          (core/synchronize test)
          (reset! running (->> test :nodes (map #(vector % false)) (into {})))

          ; All nodes synchronize at each startup attempt.
          (while (do (core/synchronize test)
                     (when (= node (core/primary test))
                       (info "Running nodes:" @running))
                     (not-every? @running (:nodes test)))

            ; Nuke local node; we've got to start fresh if we got ourselves
            ; wedged last time.
            (db/teardown! this test node)
            (core/synchronize test)

            ; Launch primary first
            (when (= node (core/primary test))
              (start-etcd! test node)
              (Thread/sleep 1000))

            ; Launch secondaries in any order after the primary...
            (core/synchronize test)

            ; ... but with some time between each to avoid triggering the join
            ; race condition
            (when-not (= node (core/primary test))
              (locking running
                (Thread/sleep 1000)
                (start-etcd! test node)))

            ; Good news is these puppies crash quick, so we don't have to
            ; wait long to see whether they made it.
            (Thread/sleep 2000)
            (swap! running assoc node (running?)))

          (info node "etcd cluster ready.")
          (Thread/sleep 1000)
          (info (c/exec :curl :-L "http://127.0.0.1:4001/v2/machines"))))

      (teardown! [_ test node]
        (c/su
          (meh (c/exec :killall :-9 :etcd))
          (c/exec :rm :-rf pidfile data-dir log-file))
        (info node "etcd nuked")))))
