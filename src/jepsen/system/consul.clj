(ns jepsen.system.consul
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

(def binary "/usr/local/bin/consul")
(def pidfile "/var/run/consul.pid")
(def data-dir "/var/lib/etcd")
(def log-file "/var/log/etcd.log")

(defn peer-addr [node]
  (str (name node) ":7001"))

(defn addr [node]
  (str (name node) ":4001"))

(defn peers
  "The command-line peer list for an consul cluster."
  [test]
  (->> test
       :nodes
       (map peer-addr)
       (str/join ",")))

(defn running?
  "Is consul running?"
  []
  (try
    (c/exec :start-stop-daemon :--status
            :--pidfile pidfile
            :--exec binary)
    true
    (catch RuntimeException _ false)))

(defn start-consul!
  [test node]
  (info node "starting consul")
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
                (when-not (cu/file? "consul")
                  (info node "cloning consul")
                  (c/exec :git :clone "https://github.com/hashicorp/consul")))

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
              (start-consul! test node)
              (Thread/sleep 1000))

            ; Launch secondaries in any order after the primary...
            (core/synchronize test)

            ; ... but with some time between each to avoid triggering the join
            ; race condition
            (when-not (= node (core/primary test))
              (locking running
                (Thread/sleep 1000)
                (start-consul! test node)))

            ; Good news is these puppies crash quick, so we don't have to
            ; wait long to see whether they made it.
            (Thread/sleep 2000)
            (swap! running assoc node (running?)))

          (info node "etcd ready")))

      (teardown! [_ test node]
        (c/su
          (meh (c/exec :killall :-9 :consul))
          (c/exec :rm :-rf data-dir log-file))
        (info node "consul nuked")))))

(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (let [client (v/connect (str "http://" (name node) ":4001"))]
      (v/reset! client k (json/generate-string nil))
      (assoc this :client client)))

  (invoke! [this test op]
    (try+
      (case (:f op)
        :read  (try (let [value (-> client
                                    (v/get k {:consistent? true})
                                    (json/parse-string true))]
                      (assoc op :type :ok :value value))
                    (catch Exception e
                      (warn e "Read failed")
                      ; Since reads don't have side effects, we can always
                      ; pretend they didn't happen.
                      (assoc op :type :fail)))

        :write (do (->> (:value op)
                        json/generate-string
                        (v/reset! client k))
                   (assoc op :type :ok))

        :cas   (let [[value value'] (:value op)
                     ok?            (v/cas! client k
                                            (json/generate-string value)
                                            (json/generate-string value'))]
                 (assoc op :type (if ok? :ok :fail))))

      (catch (and (:errorCode %) (:message %)) e
        (assoc op :type :info :value e))))

  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single consul node."
  []
  (CASClient. "jepsen" nil))
