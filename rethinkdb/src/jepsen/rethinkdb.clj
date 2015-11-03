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
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [rethinkdb.core :refer [connect close]]
            [rethinkdb.query :as r]
            [knossos.core :as knossos]
            [cheshire.core :as json])
  (:import (clojure.lang ExceptionInfo)))

(defn copy-from-home [file]
    (c/scp* (str (System/getProperty "user.home") "/" file) "/root"))

(defn db
  "Rethinkdb (ignores version)."
  []
  (reify db/DB

    (setup! [_ test node]
      (debian/install [:libprotobuf7 :libicu48 :psmisc])
      (info node "Starting...")
      ;; TODO: detect server failing to start.
      (c/exec :killall :rethinkdb :bash)
      (c/exec (clojure.string/join
                     ["JOINLINE='",
                      (clojure.string/join
                       " "
                       (map (fn [x] (format "-j %s:29015" (name x))) (:nodes test))),
                      "'\n",
                      (slurp (io/resource "setup.sh"))]))
      (info node "Starting DONE!"))

    (teardown! [_ test node]
      (info node "Tearing down db...")
      (c/ssh* {:cmd (slurp (io/resource "teardown.sh"))})
      (info node "Tearing down db DONE!"))))

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
       ; A server selection error means we never attempted the operation in
       ; the first place, so we know it didn't take place.
       (catch clojure.lang.ExceptionInfo e#
         (condp get (:type (ex-data e#))
           #{:op-indeterminate :unknown} (assoc ~op :type :info :error (str e#))
           (assoc ~op :type :fail :error (str e#)))))))

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
           :db        (db)
           :model     (model/cas-register)
           :checker   (checker/compose {:linear checker/linearizable})
           :nemesis   (nemesis/partition-random-halves))
    opts))
