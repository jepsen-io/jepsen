(ns jepsen.role-test
  (:refer-clojure :exclude [test])
  (:require [clojure [test :refer :all]]
            [jepsen [db :as db]
                    [db-test :refer [log-db]]
                    [role :as r]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def test
  "A basic test map."
  {:nodes ["a" "b" "c" "d" "e"]
   :roles {"a" :coord
           "b" :txn
           "c" :txn
           "d" :storage
           "e" :storage}})

(deftest role-test
  (is (= :coord (r/role test "a")))
  (is (= :storage (r/role test "e")))
  (is (= {:type :jepsen.role/no-role-for-node
          :node "fred"
          :roles (:roles test)}
         (try+ (r/role test "fred")
               (catch map? e e)))))

(deftest nodes-test
  (is (= ["a"] (r/nodes test :coord)))
  (is (= ["b" "c"] (r/nodes test :txn)))
  (is (= ["d" "e"] (r/nodes test :storage)))
  (is (= [] (r/nodes test :cat))))

(deftest restrict-test-test
  (is (= {:nodes ["b" "c"]
          :roles (:roles test)}
         (r/restrict-test test :txn))))

(deftest db-test
  ; DBs are basically all stateful, so we simulate a bunch of stateful calls
  ; and check to see what it proxied to.
  (let [log (atom [])
        db  (r/db {:coord   (log-db log :coord)
                   :txn     (log-db log :txn)
                   :storage (log-db log :storage)})
        coord-test   (r/restrict-test test :coord)
        txn-test     (r/restrict-test test :txn)
        storage-test (r/restrict-test test :storage)
        drain-log! (fn drain-log! []
                     (let [l @log]
                       (reset! log [])
                       l))]
    (testing "setup"
      (db/setup! db test "a")
      (db/setup! db test "b")
      (is (= [[:coord :setup! coord-test "a"]
              [:txn   :setup! txn-test   "b"]]
             (drain-log!))))

    (testing "teardown"
      (db/teardown! db test "c")
      (db/teardown! db test "d")
      (is (= [[:txn :teardown! txn-test "c"]
              [:storage :teardown! storage-test "d"]]
             (drain-log!))))

    (testing "kill"
      (db/kill! db test "e")
      (db/start! db test "a")
      (is (= [[:storage :kill! storage-test "e"]
              [:coord :start!  coord-test "a"]]
             (drain-log!))))

    (testing "pause"
      (db/pause!  db test "a")
      (db/resume! db test "b")
      (is (= [[:coord :pause! coord-test "a"]
              [:txn   :resume! txn-test "b"]]
             (drain-log!))))

    (testing "primaries"
      (is (= ["a" "b" "d"] (db/primaries db test))))

    (testing "setup-primary!"
      (db/setup-primary! db test "a")
      (is (= #{[:coord   :setup-primary! coord-test "a"]
               [:txn     :setup-primary! txn-test "b"]
               [:storage :setup-primary! storage-test "d"]}
             (set (drain-log!)))))

    (testing "log-files"
      (is (= [:coord]   (db/log-files db test "a")))
      (is (= [:storage] (db/log-files db test "e"))))
    ))
