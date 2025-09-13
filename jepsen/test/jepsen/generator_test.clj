(ns jepsen.generator-test
  (:require [jepsen.generator [context :as ctx]
                              [test :as gen.test]]
            [jepsen [generator :as gen]
                    [history :as h]
                    [independent :as independent]
                    [util :as util]]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (io.lacuna.bifurcan IMap
                               Map
                               Set)))

;; Independent tests

(deftest independent-sequential-test
  (is (= [[0 0 [:x 0]]
          [0 1 [:x 1]]
          [10 1 [:x 2]]
          [10 0 [:y 0]]
          [20 0 [:y 1]]
          [20 1 [:y 2]]]
         (->> (independent/sequential-generator
                [:x :y]
                (fn [k]
                  (->> (range)
                       (map (partial hash-map :type :invoke, :value))
                       (gen/limit 3))))
              gen/clients
              gen.test/perfect
              (map (juxt :time :process :value))))))

(deftest independent-concurrent-test
  ; All 3 groups can concurrently execute the first 2 values from k0, k1, k2
  (is (= [[0 4 [:k2 :v0]]
          [0 5 [:k2 :v1]]
          [0 0 [:k0 :v0]]
          [0 3 [:k1 :v0]]
          [0 2 [:k1 :v1]]
          [0 1 [:k0 :v1]]

          ; Worker 1 in group 0 finishes k0
          [10 1 [:k0 :v2]]
          ; Worker 2 in group 1 finishes k1
          [10 2 [:k1 :v2]]
          ; Worker 3 in group 1 starts k3, since k1 is done
          [10 3 [:k3 :v0]]
          ; And worker 0 in group 0 starts k4, since k0 is done
          [10 0 [:k4 :v0]]
          ; And worker 5 in group 2 finishes k2
          [10 5 [:k2 :v2]]

          ; All keys have now started execution. Group 1 (workers 2 and 3) is
          ; free, but can't start a new key since there are none left pending.
          ; Worker 0 in group 0 can continue k4
          [20 0 [:k4 :v1]]
          ; Workers 2 and 3 in group 1 finish off k3
          [20 3 [:k3 :v1]]
          [20 2 [:k3 :v2]]
          ; Finally, process 1 in group 0 finishes k4
          [20 1 [:k4 :v2]]]
         (->> (independent/concurrent-generator
                2                     ; 2 threads per group
                [:k0 :k1 :k2 :k3 :k4] ; 5 keys
                (fn [k]
                  (->> [:v0 :v1 :v2] ; Three values per key
                       (map (partial hash-map :type :invoke, :value)))))
              (gen.test/perfect (gen.test/n+nemesis-context 6)) ; 3 groups of 2 threads each
              (map (juxt :time :process :value))))))

(deftest independent-deadlock-case
  (is (= [[0 0 :meow [0 nil]]
          [0 1 :meow [0 nil]]
          [10 1 :meow [1 nil]]
          [10 0 :meow [1 nil]]
          [20 1 :meow [2 nil]]]
          (->> (independent/concurrent-generator
                2
                (range)
                (fn [k] (gen/each-thread {:f :meow})))
              (gen/limit 5)
              gen/clients
              gen.test/perfect
              (map (juxt :time :process :f :value))))))
