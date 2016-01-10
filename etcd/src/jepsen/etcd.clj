(ns jepsen.etcd
  (:require [clojure.tools.logging    :refer [debug info warn]]
            [clojure.java.io          :as io]
            [clojure.string           :as str]
            [jepsen.core              :as core]
            [jepsen.util              :refer [meh timeout]]
            [jepsen.codec             :as codec]
            [jepsen.core              :as core]
            [jepsen.control           :as c]
            [jepsen.control.net       :as net]
            [jepsen.control.util      :as cu]
            [jepsen.client            :as client]
            [jepsen.db                :as db]
            [jepsen.generator         :as gen]
            [jepsen.os.debian         :as debian]
            [knossos.core             :as knossos]
            [cheshire.core            :as json]
            [slingshot.slingshot      :refer [try+]]
            [verschlimmbesserung.core :as v]))

(def binary "/opt/etcd/bin/etcd")
(def pidfile "/var/run/etcd.pid")
(def data-dir "/var/lib/etcd")
(def log-file "/var/log/etcd.log")

(defn peer-addr [node]
  (str (name node) ":2380"))

(defn addr [node]
  (str (name node) ":2380"))

(defn cluster-url [node]
  (str "http://" (name node) ":2380"))

(defn listen-client-url [node]
  (str "http://" (name node) ":2379"))

(defn cluster-info [node]
  (str (name node) "=http://" (name node) ":2380"))

(defn peers
  "The command-line peer list for an etcd cluster."
  [test]
  (->> test
       :nodes
       (map cluster-info)
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
          :-data-dir        data-dir
          :-name            (name node)
          :-advertise-client-urls (cluster-url node)
          :-listen-peer-urls (cluster-url node)
          :-listen-client-urls (listen-client-url node)
          :-initial-advertise-peer-urls (cluster-url node)
          :-initial-cluster-state "new"
          :-initial-cluster (peers test)
          :>>               log-file
          (c/lit "2>&1")))

(defn db []
  (let [running (atom nil)] ; A map of nodes to whether they're running
    (reify db/DB
      (setup! [this test node]
        ; You'll need debian testing for this
        (debian/install [:git-core])

        ; Install golang 1.5.2
        (c/su
          (c/cd "/opt"
                (when-not (cu/file? "go")
                  (info node "installing golang 1.5.2")
                  (c/exec :wget :-c "https://storage.googleapis.com/golang/go1.5.2.linux-amd64.tar.gz")
                  (c/exec :tar :xzf "go1.5.2.linux-amd64.tar.gz")
                  (c/exec :rm :-f "go1.5.2.linux-amd64.tar.gz"))))
        
        (c/su
          (c/cd "/opt"
                (when-not (cu/file? "etcd")
                  (info node "cloning etcd")
                  (c/exec :git :clone "https://github.com/coreos/etcd")))

          (c/cd "/opt/etcd"
                (when-not (cu/file? "bin/etcd")
                  (info node "building etcd")
                  (c/exec :env (c/lit "GOROOT=/opt/go") (c/lit "PATH=$PATH:/opt/go/bin")
                          (c/lit "./build"))))

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
                (Thread/sleep 100)
                (start-etcd! test node)))

            ; Good news is these puppies crash quick, so we don't have to
            ; wait long to see whether they made it.
            (Thread/sleep 2000)
            (swap! running assoc node (running?)))

          ; And spin some more until Raft is ready
          (let [c (v/connect (str "http://" (name node) ":2379"))]
            (while (try+ (v/reset! c :test "ok") false
                         (catch [:status 500] e true)
                         (catch [:status 307] e true))
              (Thread/sleep 100)))

          (info node "etcd ready")))

      (teardown! [_ test node]
        (c/su
          (meh (c/exec :killall :-9 :etcd))
          (c/exec :rm :-rf pidfile data-dir log-file))
        (info node "etcd nuked")))))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (let [client (v/connect (str "http://" (name node) ":2379"))]
      (v/reset! client k (json/generate-string nil))
      (assoc this :client client)))

  (invoke! [this test op]
    ; Reads are idempotent; if they fail we can always assume they didn't
    ; happen in the history, and reduce the number of hung processes, which
    ; makes the knossos search more efficient
    (let [fail (if (= :read (:f op))
                 :fail
                 :info)]
      (try+
        (case (:f op)
          :read  (let [value (-> client
                                 (v/get k {:consistent? true})
                                 (json/parse-string true))]
                   (assoc op :type :ok :value value))

          :write (do (->> (:value op)
                          json/generate-string
                          (v/reset! client k))
                     (assoc op :type :ok))

          :cas   (let [[value value'] (:value op)
                       ok?            (v/cas! client k
                                              (json/generate-string value)
                                              (json/generate-string value'))]
                   (assoc op :type (if ok? :ok :fail))))

        ; A few common ways etcd can fail
        (catch java.net.SocketTimeoutException e
          (assoc op :type fail :value :timed-out))

        (catch [:body "command failed to be committed due to node failure\n"] e
          (assoc op :type fail :value :node-failure))

        (catch [:status 307] e
          (assoc op :type fail :value :redirect-loop))

        (catch (and (instance? clojure.lang.ExceptionInfo %)) e
          (assoc op :type fail :value e))

        (catch (and (:errorCode %) (:message %)) e
          (assoc op :type fail :value e)))))

  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single etcd node."
  []
  (CASClient. "jepsen" nil))
