(ns jepsen.role-test
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [client :as client]
                    [db :as db]
                    [db-test :refer [log-db]]
                    [generator :as gen]
                    [role :as r]
                    [nemesis :as n]]
            [jepsen.generator.test :as gt]
            [jepsen.nemesis.combined :as nc]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent CyclicBarrier)))

(def test
  "A basic test map."
  {:nodes ["a" "b" "c" "d" "e"]
   :roles {:coord   ["a"]
           :txn     ["b" "c"]
           :storage ["d" "e"]}
   :concurrency 5
   :barrier (CyclicBarrier. 5)})

(defn test=
  "Special comparator for tests, equal in all but barrier"
  [a b]
  (and (= (dissoc a :barrier) (dissoc b :barrier))
       (= (.getParties (:barrier a)) (.getParties (:barrier b)))))

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
  (is (= nil (r/nodes test :cat))))

(deftest restrict-test-test
  (let [t (r/restrict-test :txn test)]
    (testing "nodes"
      (is (= ["b" "c"] (:nodes t))))
    (testing "roles"
      (is (= (:roles test) (:roles t))))
    (testing "concurrency"
      (is (= 5 (:concurrency t))))
    (testing "barrier"
      ; When doing DB setup, sub-nodes calling synchronize can't wait for the
      ; full set!
      (is (= 2 (.getParties (:barrier t)))))))

(deftest db-test
  ; DBs are basically all stateful, so we simulate a bunch of stateful calls
  ; and check to see what it proxied to.
  (let [log (atom [])
        db  (r/db {:coord   (log-db log :coord)
                   :txn     (log-db log :txn)
                   :storage (log-db log :storage)})
        coord-test   (r/restrict-test :coord test)
        txn-test     (r/restrict-test :txn test)
        storage-test (r/restrict-test :storage test)
        drain-log! (fn drain-log! []
                     (let [l @log]
                       (reset! log [])
                       l))
        ; Special comparator for single log lines, equal in all but barrier
        ll= (fn ll= [a b]
              (and (= 4 (count a) (count b))
                   (= (nth a 0) (nth b 0))
                   (= (nth a 1) (nth b 1))
                   (test= (nth a 2) (nth b 2))
                   (= (nth a 3) (nth b 3))))
        ; Special comparator for groups of log lines, equal in all but barrier
        l= (fn l= [as bs]
             (and (= (count as) (count bs))
                  (every? true? (map ll= as bs))))]

    (testing "setup"
      (db/setup! db test "a")
      (db/setup! db test "b")
      (is (l= [[:coord :setup! coord-test "a"]
               [:txn   :setup! txn-test   "b"]]
              (drain-log!))))

    (testing "teardown"
      (db/teardown! db test "c")
      (db/teardown! db test "d")
      (is (l= [[:txn :teardown! txn-test "c"]
              [:storage :teardown! storage-test "d"]]
             (drain-log!))))

    (testing "kill"
      (db/kill! db test "e")
      (db/start! db test "a")
      (is (l= [[:storage :kill! storage-test "e"]
              [:coord :start!  coord-test "a"]]
             (drain-log!))))

    (testing "pause"
      (db/pause!  db test "a")
      (db/resume! db test "b")
      (is (l= [[:coord :pause! coord-test "a"]
              [:txn   :resume! txn-test "b"]]
             (drain-log!))))

    (testing "primaries"
      (is (= ["a" "b" "d"] (db/primaries db test))))

    (testing "setup-primary!"
      (db/setup-primary! db test "a")
      (is (l= [[:coord   :setup-primary! coord-test "a"]
               [:storage :setup-primary! storage-test "d"]
               [:txn     :setup-primary! txn-test "b"]]
              (sort (drain-log!)))))

    (testing "log-files"
      (is (= [:coord]   (db/log-files db test "a")))
      (is (= [:storage] (db/log-files db test "e"))))
    ))

(deftest restrict-client-test
  ; Clients need to remap nodes when calling open!
  (let [c (reify client/Client
            (open! [this test node]
              [:client node]))
        wrapper (r/restrict-client :txn c)]
    (is (= [:client "b"] (client/open! wrapper test "a")))
    (is (= [:client "c"] (client/open! wrapper test "b")))
    (is (= [:client "b"] (client/open! wrapper test "c")))
    (is (= [:client "c"] (client/open! wrapper test "d")))
    (is (= [:client "b"] (client/open! wrapper test "e")))
    ))

; A trivial nemesis, just to verify we restrict tests properly.
(defrecord Nemesis []
  n/Nemesis
  (setup! [this test]
    (assoc this :setup-test test))

  (invoke! [this test op]
    [test op])

  (teardown! [this test]
    test))

(deftest restrict-nemesis-test
  (let [n (r/restrict-nemesis :storage (Nemesis.))
        rt (r/restrict-test :storage test)]
    (is (test= rt (:setup-test (:nemesis (n/setup! n test)))))
    (let [[test' op'] (n/invoke! n test :foo)]
      (is (test= rt test'))
      (is (= :foo op')))
    (is (test= rt (n/teardown! n test)))))

(deftest restrict-nemesis-package-test
  (let [pkg (nc/partition-package {:faults #{:partition}})
        pkg' (r/restrict-nemesis-package :storage pkg)]
    ; We have tests for the nemesis already; just check the generator lifts
    ; properly
    (testing "generator"
      (let [ops (->> (:generator pkg')
                     gen/nemesis
                     (gen/limit 2)
                     gt/perfect*
                     (filter (comp #{:info} :type))
                     ; values use rand-nth baked into fns in a way we can't
                     ; make deterministic; just drop em
                     (map #(dissoc % :value)))]
        (is (= [{:time 0,
                 :type :info,
                 :process :nemesis,
                 :f [:storage :start-partition]}
                {:time 15702284397,
                 :type :info,
                 :process :nemesis,
                 :f [:storage :stop-partition]}]
               ops))))))
