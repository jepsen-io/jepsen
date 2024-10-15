(ns jepsen.db-test
  "Tests for jepsen.db"
  (:require [clojure [test :refer :all]]
            [jepsen [db :as db]]))

(defn log-db
  "A DB which logs operations of the form [:prefix op test node] to the given atom, containing a vector."
  [log prefix]
  (reify db/DB
    (setup!     [_ test node] (swap! log conj [prefix :setup! test node]))
    (teardown!  [_ test node] (swap! log conj [prefix :teardown! test node]))

    db/Kill
    (start!     [_ test node] (swap! log conj [prefix :start! test node]))
    (kill!      [_ test node] (swap! log conj [prefix :kill! test node]))

    db/Pause
    (pause!     [_ test node] (swap! log conj [prefix :pause! test node]))
    (resume!    [_ test node] (swap! log conj [prefix :resume! test node]))

    db/Primary
    (primaries   [_ test]         [(first (:nodes test))])
    (setup-primary! [_ test node] (swap! log conj [prefix :setup-primary! test node]))

    db/LogFiles
    (log-files [db test node]
      [prefix])))

(deftest map-test-test
  (let [n   "a"
        t   {:nodes [n]}
        log (atom [])
        db  (db/map-test #(assoc % :nodes ["b"])
                         (log-db log :log))
        t'  {:nodes ["b"]}]
    (testing "side effects"
      (db/setup! db t n)
      (db/teardown! db t n)
      (db/kill! db t n)
      (db/start! db t n)
      (db/pause! db t n)
      (db/resume! db t n)
      (db/setup-primary! db t n)
      (is (= [[:log :setup! t' n]
              [:log :teardown! t' n]
              [:log :kill! t' n]
              [:log :start! t' n]
              [:log :pause! t' n]
              [:log :resume! t' n]
              [:log :setup-primary! t' n]]
             @log)))
    ; We don't test primaries or log-files, maybe add this later.
    ))
