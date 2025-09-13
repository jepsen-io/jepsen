(ns jepsen.generator.context-test
  (:require [clojure [datafy :refer [datafy]]
                     [pprint :refer [pprint]]
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
            (is (= 1 (process->thread c 1)))
            (is (= 0 (some-free-process c))))

        c1 (busy-thread c 5 0)
        _ (testing "busy"
            (is (= 5 (:time c1)))
            (is (= 3 (all-thread-count c1)))
            (is (= 2 (free-thread-count c1)))
            (is (= (Set/from [:nemesis 1]) (free-threads c1)))
            (is (= #{:nemesis 1} (set (free-processes c1))))
            (is (= 1 (some-free-process c1))))

        c2 (free-thread c1 10 0)
        _ (testing "free"
            (is (= (assoc c :time 10) c2))
            ; But the free process has advanced, for fairness!
            (is (= 1 (some-free-process c2))))

        _ (testing "thread-filter"
            (testing "all-but"
              (let [c3 ((make-thread-filter (all-but :nemesis) c) c)]
                (is (= (Set/from [0 1]) (free-threads c3)))
                (is (= (Set/from [0 1]) (all-threads c3)))
                (is (= #{0 1} (set (free-processes c3))))
                (is (= #{0 1} (set (all-processes c3))))
                (is (= 0 (some-free-process c3)))))

            (testing "set"
              (let [c3 ((make-thread-filter #{1 :nemesis} c) c)]
                (is (= (Set/from [1 :nemesis]) (free-threads c3)))
                (is (= (Set/from [1 :nemesis]) (all-threads c3)))
                (is (= #{1 :nemesis} (set (free-processes c3))))
                (is (= #{1 :nemesis} (set (all-processes c3))))
                (is (= 1 (some-free-process c3)))))

            (testing "fn"
              (let [c3 ((make-thread-filter integer? c) c)]
                (is (= (Set/from [0 1]) (free-threads c3)))
                (is (= (Set/from [0 1]) (all-threads c3)))
                (is (= #{0 1} (set (free-processes c3))))
                (is (= #{0 1} (set (all-processes c3))))
                (is (= 0 (some-free-process c3))))))
        ]))

(deftest with-next-process-test
  ; As we crash threads their processes should advance
  (let [c  (context {:concurrency 2})
        c1 (with-next-process c 0)
        _  (is (= 2 (thread->process c1 0)))
        _  (is (= 0 (process->thread c1 2)))
        c2 (with-next-process c1 0)
        _  (is (= 4 (thread->process c2 0)))
        _  (is (= 0 (process->thread c2 4)))]))

(deftest some-free-process-test
  (let [c (context {:concurrency 2})]
    (testing "all free"
      (is (= 0 (some-free-process c))))

    (testing "some busy"
      (is (= 1    (-> c (busy-thread 0 0) some-free-process)))
      (is (not= 1 (-> c (busy-thread 0 1) some-free-process))))

    ; We want to make sure that if we use and free a process, and all *later*
    ; processes are busy, we realize the first process is still free.
    (testing "driven forward"
      (let [c' (-> c
                   (busy-thread 0 1)
                   (busy-thread 0 :nemesis))]
        (is (= 0 (some-free-process c')))))

    ; We want to distribute requests evenly across threads to prevent
    ; starvation.
    (testing "fair"
      (let [c1 (-> c (busy-thread 0 0) (free-thread 0 0))
            _ (is (= 1 (some-free-process c1)))
            c2 (-> c1 (busy-thread 0 1) (free-thread 0 1))
            _ (is (= :nemesis (some-free-process c2)))
            c3 (-> c2 (busy-thread 0 :nemesis) (free-thread 0 :nemesis))
            _ (is (= 0 (some-free-process c3)))]))))

(deftest assoc-test
  (let [c (context {:concurrency 2})
        c' (assoc c :special 123)]
    (is (= 123 (:special c')))
    (is (= (class c) (class c')))))
