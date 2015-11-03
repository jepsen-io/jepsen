(ns jepsen.rethinkdb
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [rethinkdb.core :refer [connect close]]
            [rethinkdb.query :as r]
            [knossos.core :as knossos]
            [cheshire.core :as json])
  (:import (clojure.lang ExceptionInfo)))

(defn copy-from-home [file]
    (c/scp* (str (System/getProperty "user.home") "/" file) "/root"))

(defn install!
  "Install RethinkDB on a node"
  [node version]
  (debian/add-repo! "rethinkdb"
                    "deb http://download.rethinkdb.com/apt jessie main")
  (c/su (c/exec :wget :-qO :- "https://download.rethinkdb.com/apt/pubkey.gpg" |
                :apt-key :add :-))
  (debian/install {"rethinkdb" version}))

(defn join-line
  "Command-line arguments for nodes to join the cluster."
  [test]
  (->> test
       :nodes
       (map name)
       (map (partial format "-j %s:29015"))
       (clojure.string/join " ")))

(defn db
  "Set up and tear down RethinkDB"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (install! node version))

    (teardown! [_ test node]
      (info node "Nuking RethinkDB")
      (cu/grepkill! "rethinkdb")
      (info node "RethinkDB dead"))))

(defmacro with-errors
  "Takes an invocation operation, a set of idempotent operation
  functions which can be safely assumed to fail without altering the
  model state, and a body to evaluate. Catches RethinkDB errors and
  maps them to failure ops matching the invocation."
  [op idempotent-ops & body]
  `(let [error-type# (if (~idempotent-ops (:f ~op))
                       :fail
                       :info)]
     (try
       ~@body
       (catch clojure.lang.ExceptionInfo e#
         (condp get (:type (ex-data e#))
           #{:op-indeterminate :unknown} (assoc ~op :type :info :error (str e#))
           (assoc ~op :type :fail error-type# (str e#)))))))

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  [gen]
  (gen/phases
    (->> gen
         (gen/delay 1)
         (gen/nemesis
           (gen/seq (cycle [(gen/sleep 60)
                            {:type :info :f :stop}
                            {:type :info :f :start}])))
         (gen/time-limit 600))
    ;; Recover
    (gen/nemesis
      (gen/once {:type :info :f :stop}))
    ;; Wait for resumption of normal ops
    (gen/clients
      (->> gen
           (gen/delay 1)
           (gen/time-limit 30)))))

(defn test-
  "Constructs a test with the given name prefixed by 'rdb ', merging any
  given options."
  [name opts]
  (merge
    (assoc tests/noop-test
           :name      (str "rethinkdb " name)
           :os        debian/os
           :db        (db (:version opts))
           :model     (model/cas-register)
           :checker   (checker/compose {:linear checker/linearizable})
           :nemesis   (nemesis/partition-random-halves))
    (dissoc opts :version)))
