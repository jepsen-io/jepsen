(ns jepsen.generator.context-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen.generator.context :refer :all])
  (:import (io.lacuna.bifurcan IMap
                               Map
                               Set)))

(deftest context-test
  (let [c (context {:concurrency 2})
        _ (testing "basics"
            (is (= 0 (:time c)))
            (is (= 3 (all-thread-count c)))
            (is (= 3 (free-thread-count c)))
            (is (= (Set/from [:nemesis 0 1]) (free-threads c)))
            (is (= #{:nemesis 0 1} (set (free-processes c))))
            (is (= :nemesis (thread->process c :nemesis)))
            (is (= :nemesis (process->thread c :nemesis)))
            (is (= 1 (thread->process c 1)))
            (is (= 1 (process->thread c 1))))

        c1 (busy-thread c 5 0)
        _ (testing "busy"
            (is (= 5 (:time c1)))
            (is (= 3 (all-thread-count c1)))
            (is (= 2 (free-thread-count c1)))
            (is (= (Set/from [:nemesis 1]) (free-threads c1)))
            (is (= #{:nemesis 1} (set (free-processes c1)))))

        c2 (free-thread c1 10 0)
        _ (testing "free"
            (is (= (assoc c :time 10) c2)))

        _ (testing "on-threads-context"
            (testing "all-but"
              (let [c3 (on-threads-context (all-but :nemesis) c)]
                (is (= (Set/from [0 1]) (free-threads c3)))
                (is (= (Set/from [0 1]) (all-threads c3)))
                (is (= #{0 1} (set (free-processes c3))))
                (is (= #{0 1} (set (all-processes c3))))))
            (testing "set"
              (let [c3 (on-threads-context #{1 :nemesis} c)]
                (is (= (Set/from [1 :nemesis]) (free-threads c3)))
                (is (= (Set/from [1 :nemesis]) (all-threads c3)))
                (is (= #{1 :nemesis} (set (free-processes c3))))
                (is (= #{1 :nemesis} (set (all-processes c3))))))
            (testing "fn"
              (let [c3 (on-threads-context integer? c)]
                (is (= (Set/from [0 1]) (free-threads c3)))
                (is (= (Set/from [0 1]) (all-threads c3)))
                (is (= #{0 1} (set (free-processes c3))))
                (is (= #{0 1} (set (all-processes c3)))))))
        ]))
