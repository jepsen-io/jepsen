(ns jepsen.nemesis-test
  (:use clojure.test
        clojure.pprint
        jepsen.nemesis)
  (:require [jepsen.client :as client]
            [jepsen.control :as c]
            [jepsen.control.net :as net]
            [jepsen.util :refer [meh]]
            [jepsen.tests :refer [noop-test]]))

(defn edges
  "A map of nodes to the set of nodes they can ping"
  [test]
  (c/on-many (:nodes test)
             (into (sorted-set) (filter net/reachable? (:nodes test)))))

(deftest bisect-test
  (is (= (bisect []) [[] []]))
  (is (= (bisect [1]) [[] [1]]))
  (is (= (bisect [1 2 3 4]) [[1 2] [3 4]]))
  (is (= (bisect [1 2 3 4 5]) [[1 2] [3 4 5]])))

(deftest complete-grudge-test
  (is (= (complete-grudge (bisect [1 2 3 4 5]))
         {1 #{3 4 5}
          2 #{3 4 5}
          3 #{1 2}
          4 #{1 2}
          5 #{1 2}})))

(deftest bridge-test
  (is (= (bridge [1 2 3 4 5])
         {1 #{4 5}
          2 #{4 5}
          4 #{1 2}
          5 #{1 2}})))

(deftest majorities-ring-test
  (let [nodes  (range 5)
        grudge (majorities-ring nodes)]
    (is (= (count grudge) (count nodes)))
    (is (= (set nodes) (set (keys grudge))))
    (is (every? (partial = 2) (map count (vals grudge))))
    (is (every? (fn [[node snubbed]]
                  (not-any? #{node} snubbed))
                grudge))
    (is (distinct? (vals grudge)))))

(deftest simple-partition-test)
  ;(let [n (partition-halves)]
  ;  (try
  ;    (client/setup! n noop-test nil)
  ;    (is (= (edges noop-test)
  ;           {:n1 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n2 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n3 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n4 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n5 #{:n1 :n2 :n3 :n4 :n5}}))

  ;    (client/invoke! n noop-test {:f :start})
  ;    (is (= (edges noop-test)
  ;           {:n1 #{:n1 :n2}
  ;             :n2 #{:n1 :n2}
  ;             :n3 #{:n3 :n4 :n5}
  ;             :n4 #{:n3 :n4 :n5}
  ;             :n5 #{:n3 :n4 :n5}}))

  ;    (client/invoke! n noop-test {:f :stop})
  ;    (is (= (edges noop-test)
  ;           {:n1 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n2 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n3 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n4 #{:n1 :n2 :n3 :n4 :n5}
  ;            :n5 #{:n1 :n2 :n3 :n4 :n5}}))

  ;    (finally
  ;      (client/teardown! n noop-test)))))
