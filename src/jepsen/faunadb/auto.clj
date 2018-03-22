(ns jepsen.faunadb.auto
  "FaunaDB automation functions, for starting, stopping, etc."
  (:require [clojure.tools.logging :refer :all]
            [jepsen [core :as jepsen]
                    [util :as util]
                    [control :as c :refer [|]]]))

(defn start!
  "Start faunadb on node."
  [test node]
  (if (not= "faunadb stop/waiting"
            (c/exec :initctl :status :faunadb))
    (info node "FaunaDB already running.")
    (do (info node "Starting FaunaDB...")
        (c/su
         (c/exec :initctl :start :faunadb)
         (Thread/sleep 30000)
         (jepsen/synchronize test)

         (when (= node (jepsen/primary test))
           (info node "initializing FaunaDB cluster")
           (c/exec :faunadb-admin :init)
           (Thread/sleep 10000)))
        (jepsen/synchronize test)

        (when (not= node (jepsen/primary test))
          (info node "joining FaunaDB cluster")
          (c/exec :faunadb-admin :join (jepsen/primary test))
          (Thread/sleep 10000))
        (jepsen/synchronize test)
        (info node "FaunaDB started")))
  :started)

(defn kill!
  "Kills FaunaDB on node."
  [test node]
  (util/meh (c/su (c/exec :killall :-9 :java)))
  (info node "FaunaDB killed.")
  :killed)

(def ntpserver "ntp.ubuntu.com")

(defn reset-clock!
  "Reset clock on this host. Logs output."
  []
  (info c/*host* "clock reset:" (c/su (c/exec :ntpdate :-b ntpserver))))

(defn reset-clocks!
  "Reset all clocks on all nodes in a test"
  [test]
  (c/with-test-nodes test (reset-clock!)))
