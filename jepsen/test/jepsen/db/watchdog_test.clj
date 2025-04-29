(ns jepsen.db.watchdog-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [client :as client]
                    [common-test :refer [quiet-logging]]
                    [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [generator :as gen]
                    [history :as h]
                    [nemesis :as n]
                    [tests :refer [noop-test]]]
            [jepsen.control [util :as cu]]
            [jepsen.db.watchdog :as w]
            [slingshot.slingshot :refer [try+ throw+]]))

(use-fixtures :once quiet-logging)

; This DB likes to crash all the time
(defrecord CrashDB []
  db/DB
  (setup! [this test node]
    (db/start! this test node))

  (teardown! [this test node]
    (db/kill! this test node))

  db/Kill
  (kill! [_ test node]
    (cu/grepkill! "sleep"))

  (start! [_ test node]
    (cu/start-daemon! {:chdir   "/tmp/jepsen"
                       :pidfile "/tmp/jepsen/watchdog-test.pid"
                       :logfile "/tmp/jepsen/watchdog-test.log"}
                      "/usr/bin/sleep" 3))

  db/LogFiles
  (log-files [_ _ _])

  db/Primary
  (primaries [_ _])
  (setup-primary! [_ _ _]))

(defn running?
  "Is the DB running?"
  [test node]
  (try+ (c/exec :pgrep "sleep")
        true
        (catch [:type :jepsen.control/nonzero-exit] _
          false)))

; Just looks to see if it's running or not
(defrecord Client []
  client/Client
  (open! [this test node]
    this)

  (setup! [_ _])

  (invoke! [_ test op]
    (let [r (c/on-nodes test running?)
          ; We're going to assume every node runs on the same schedule, at least at our granularity.
          _ (assert (apply = (vals r)))
          r (val (first r))]
      (assoc op :type :ok, :value r)))

  (teardown! [_ _])

  (close! [_ _]))

; No way around this being slow, we actually have to run stuff on real nodes
; and wait for it to die.
(deftest ^:slow ^:integration watchdog-test
  (let [; We poll every second to see if things are running
        gen (->> (gen/repeat {:f :running})
                 (gen/delay 1)
                 ; We wait five seconds, kill everyone, wait five more, and restart.
                 (gen/nemesis
                   [(gen/sleep 5)
                    {:type :info, :f :start}
                    (gen/sleep 5)
                    {:type :info, :f :stop}])
                 (gen/time-limit 15))
        ; If you remove the w/db wrapper, the test will fail
        db   (w/db {:running? running?}
                   (CrashDB.))
        test (assoc noop-test
                    :nodes     ["n1"]
                    :name      "watchdog-test"
                    :db        db
                    :client    (Client.)
                    :nemesis   (n/node-start-stopper identity
                                                     (partial db/kill! db)
                                                     (partial db/start! db))
                    :generator gen)
        test' (jepsen/run! test)
        ; Project down history
        h (->> (:history test')
               (h/remove h/invoke?)
               (h/map :value)
               (remove nil?))
        ; Divide the polls into three zones: the fresh cluster, once killed,
        ; and once restarted.
        [fresh _ dead _ restarted] (partition-by map? h)]

    ;(pprint fresh)
    ;(pprint dead)
    ;(pprint restarted)

    (testing "fresh"
      ; Initially, we should be running.
      (is (first fresh))
      ; But even though we die, we should be restarted and running at the end.
      (is (last fresh)))

    (testing "dead"
      ; We should never run during this part. We might race during the first op though.
      (is (every? false? (next dead))))

    (testing "restarted"
      ; Same deal, we should be running at the start and end.
      (is (first fresh))
      (is (last fresh)))
    ))
