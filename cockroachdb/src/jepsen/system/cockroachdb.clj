(ns jepsen.system.cockroachdb
  (:require [clojure.tools.logging    :refer [debug info warn]]
            [jepsen.client            :as client]
            [jepsen.db                :as db]
            [jepsen.control           :as c]
            [jepsen.control.util      :as cu]
            [jepsen.util              :refer [meh timeout]]
            [jepsen.core              :as core]
            [jepsen.os.debian         :as debian]
            [cockroach.client         :as r]
            [cheshire.core            :as json]
            [slingshot.slingshot      :refer [try+]]))

(def working-path "/opt/cockroach")
(def data-dir "/opt/cockroach/store")
(def pidfile "/opt/cockroach/pid")
(def logfile "/opt/cockroach/log")
(def gopath "/opt/cockroach/gopath")
(def cockroach-path "/opt/cockroach/gopath/src/github.com/cockroachdb/cockroach")
(def cockroach-repo "https://github.com/cockroachdb/cockroach/")
; golang-go is an additional dev but needs some special handling.
(def deps [:git-core :make :g++ :mercurial :bzr :zlib1g-dev :libsnappy-dev :libbz2-dev :libgflags-dev])

(defn db []
  (reify db/DB
    (setup! [_ test node]
      (when-not (debian/installed? deps)
        (info node "installing dependencies")
        ( debian/install deps)) 
      (c/trace (c/su
        (meh (c/exec :mkdir :-p gopath))
        (meh (c/exec :mkdir :-p cockroach-path))
        (let [env (str "GOPATH=" gopath)]
          (c/cd cockroach-path
            (when-not (cu/file? :.git)
              (info node "cloning cockroach")
              (c/exec :git :clone :--depth 1 :--branch :master cockroach-repo cockroach-path))
            (when-not (cu/file? "_vendor/rocksdb/README.md")
              (info node "pulling rocksdb")
              (c/exec :git :submodule :init)
              (c/exec :git :submodule :update))
            (when-not (cu/file? "cockroach")
              (meh (debian/install :golang-go))
              (info node "getting up-to-date go")
              (c/exec env :go :get "gopkg.in/niemeyer/godeb.v1/cmd/godeb")
              (c/exec :apt-get :remove :-y :golang-go)
              (c/cd gopath
                (meh (c/exec "bin/godeb" :install)))
              ; This causes an error as rocksdb.h is not found, but
              ; that can safely be ignored.
              ; (meh (c/exec env :go :get "./..."))
              (info node "building cockroachdb")
              (c/exec env :make :build))
            (info node "initializing storage engines")
            (when (= node (core/primary test))
              ; The primary node gets to bootstrap a range
              ; TODO: more refined setup, we want to test
              ; crossing the range boundaries eventually
              (info node "initializing cluster")
              ; TODO remove :-alsologtostderr (after bug in repo is fixed)
              (c/exec "./cockroach" :init :-alsologtostderr (str "ssd=" data-dir)))

            (info node "starting cockroachdb")
            (c/exec :start-stop-daemon :--start
                    :--background
                    :--make-pidfile
                    :--pidfile  pidfile
                    :--no-close
                    :--exec     (c/expand-path "./cockroach")
                    :--
                    :start
                    :-stores    (str "ssd=" data-dir)
                    :-gossip    "0.0.0.0:8081"
                    :-rpc_addr  "0.0.0.0:8081"
                    :-http_addr "0.0.0.0:8080"
                    :> logfile
                    (c/lit "2>&1"))))))

      (info node "cockroachdb started"))

    (teardown! [_ test node]
      (meh (c/exec :killall -9 :cockroach))
      (c/exec :rm :-rf data-dir pidfile)
      (info node "cockroachdb nuked"))))

(defrecord RoachClient [k, base]
  client/Client
  (setup! [this test node]
    (assoc this :base (str "http://" (name node) ":8080/db/")))

  (invoke! [this test op]
    ; Failed reads are not interesting because they don't write
    ; data, but it is smart to not tag them as a failure which
    ; makes for faster validation.
    (let [fail (if (= :read (:f op))
                 :fail
                 :info)]
      (try+
        (case (:f op)
          :read (let [value (r/read! base k)]
                  (assoc op :type :ok, :value value))

          :write (let [res (->> (:value op)
                              (r/write! base k))]
                   (if (= (:status res) 200)
                    (assoc op :type :ok)
                    (assoc op :type :fail, :value (:status res))))
          :cas nil)

        (catch java.net.SocketTimeoutException e
          (assoc op :type fail, :value :timed-out)))))

  (teardown! [_ test]))

(defn roach-client
  "A simple roach client for a single key"
  []
  (RoachClient. "cockroachdb" nil))


