(ns jepsen.zookeeper.ephemeral
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [jepsen [client :as client]
             [db :as db]
             [tests :as tests]
             [control :as c :refer [|]]
             [checker :as checker]
             [generator :as gen]
             [util :refer [timeout]]
             [zookeeper :as zookeeper]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defrecord Client []
  client/Client
  (setup! [this test node]
    this)

  (invoke! [this test op])

  (teardown! [_ test]))

(defn ephemeral-test
  [version]
  (assoc tests/noop-test
         :name "zookeeper ephemeral"
         :os   debian/os
         :db   (zookeeper/db version)
         :client (Client.)
         :generator (gen/sleep 1000000)))
