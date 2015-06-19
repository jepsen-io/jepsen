(ns jepsen.disque
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
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [knossos.core :as knossos])
  (:import (clojure.lang ExceptionInfo)
           (java.net URI)
           (com.github.xetorthio.jedisque Jedisque)))

(defn install!
  "Installs DB on the given node."
  [node version]
  (info node "installing disque")
  (debian/install [:git-core :build-essential])
  (c/su
    (c/cd "/opt"
          (when-not (cu/file? "disque")
            (c/exec :git :clone "https://github.com/antirez/disque.git")))
    (c/cd "/opt/disque"
          (c/exec :git :reset :--hard version)
          (c/exec :make))
    ))

(defn configure!
  "Uploads configuration files to the given node."
  [node test])

(defn start!
  "Starts DB."
  [node test]
  (info node "starting disque"))

(defn stop!
  "Stops DB."
  [node]
  (info node "stopping disque")
  (c/su
    (meh (c/exec :killall :-9 :disque))))

(defn wipe!
  "Shuts down the server and wipes data."
  [node]
  (stop! node)
  (info node "deleting data files"))

(defn db [version]
  "Disque for a particular version."
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! version)
        (configure! test)
        (start! test)))

    (teardown! [_ test node]
      (wipe! node))))

; Client

(def client-timeout 100)

(defrecord Client [^Jedisque client queue repl-factor]
  client/Client
  (setup! [this test node]
    (let [uri (URI. (str "disque://" (name node) ":7711"))]
      (info :uri uri)
      (assoc this
             :client (Jedisque. (into-array [uri])))))

  (invoke! [this test op]
    (case (:f op)
      :enqueue (do
                 (.addJob client queue (str (:value op)) client-timeout)
                 (assoc op :type :ok))
      :dequeue (let [job (.getJob client client-timeout 1 (into-array [queue]))]
                 (.ackJob client (into-array [(.getId job)]))
                 (assoc op
                        :type  :ok
                        :value (Long/parseLong (.getBody job))))))

  (teardown! [this test]
    (.close client)))

(defn client
  []
  (Client. nil "jepsen" 3))

; Nemeses

(defn killer
  "Kills a random node on start, restarts it on stop."
  []
  (nemesis/node-start-stopper
    rand-nth
    (fn start [test node] (c/su (c/exec :killall :-9 :asd)))
    (fn stop  [test node] (start! node test))))

; Generators

(defn std-gen
  "Takes a client generator and wraps it in a typical schedule and nemesis
  causing failover."
  [gen]
  (gen/phases
    (->> gen
         (gen/nemesis
           (gen/seq (cycle [(gen/sleep 2)
                            {:type :info :f :start}
                            (gen/sleep 5)
                            {:type :info :f :stop}])))
         (gen/time-limit 0))
    ; Recover
    (gen/nemesis (gen/once {:type :info :f :stop}))
    ; Wait for resumption of normal ops
    (gen/clients (gen/time-limit 5 gen))))

; Tests

(defn disque-test
  [name opts]
  (merge tests/noop-test
         {:name    (str "disque " name)
          :os      debian/os
          :db      (db "5df8e1d7838d7bea0bd9cf187922a1469d1bb252")
          :model   (model/unordered-queue)
          :nemesis (nemesis/partition-random-halves)
          :checker (checker/compose {:queue   checker/queue
                                     :latency (checker/latency-graph
                                                "report/")})}
         opts))

(defn basic-queue-test
  []
  (disque-test "basic"
               {:client    (client)
                :generator (->> (gen/queue)
                                (gen/delay 1)
                                std-gen)}))
