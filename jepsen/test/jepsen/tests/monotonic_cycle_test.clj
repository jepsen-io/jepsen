(ns jepsen.tests.monotonic-cycle-test
  (:require [jepsen.tests.monotonic-cycle :refer :all]
            [jepsen.checker :as checker]
            [jepsen.util :refer [map-vals]]
            [knossos.op :as op]
            [clojure.test :refer :all]
            [fipp.edn :as fipp]))

(deftest tarjan-test
  (testing "Can analyze integer graphs"
    ;; From wikipedia
    (let [graph {1 #{2}   2 #{3}
                 3 #{1}   4 #{2 3 5}
                 5 #{4 6} 6 #{3 7}
                 7 #{6}   8 #{7 8}}]
      (is (= (tarjan graph)
             #{#{3 2 1} #{6 7} #{5 4} #{8}})))

    ;; Big lööp
    (let [graph {1 #{2} 2 #{3}
                 3 #{4} 4 #{5}
                 5 #{6} 6 #{7}
                 7 #{8} 8 #{1}}]
      (is (= (tarjan graph)
             #{#{1 2 3 4 5 6 7 8}})))

    ;; smol lööps
    (let [graph {0 #{1} 1 #{0}
                 2 #{3} 3 #{2}
                 4 #{5} 5 #{4}
                 6 #{7} 7 #{6}}]
      (is (= (tarjan graph)
             #{#{0 1} #{2 3}
               #{4 5} #{6 7}}))))

  (testing "Can flag unlinked as solo sccs"
    (let [graph {1 #{} 2 #{}
                 3 #{} 4 #{}}]
      (is (= (tarjan graph)
             #{#{1} #{2} #{3} #{4}}))))

  (testing "Can flag self-ref as solo sccs"
    (let [graph {1 #{1} 2 #{2}
                 3 #{3} 4 #{4}}]
      (is (= (tarjan graph)
             #{#{1} #{2} #{3} #{4}}))))

  (testing "can check monotonic loop histories"
    ;; Linear
    (let [graph {0 #{1} 1 #{2}
                 2 #{3} 3 #{}}]
      (is (= (tarjan graph)
             #{#{0} #{1} #{2} #{3}})))

    ;; Loop
    (let [graph {0 #{1} 1 #{2}
                 2 #{1} 3 #{}}]
      (is (= (tarjan graph)
             #{#{0} #{1 2} #{3}})))

    ;; Linear but previously bugged case
    (let [graph {0 #{1} 1 #{2}
                 2 #{}  3 #{2 1}}]
      (is (= (tarjan graph)
             #{#{0} #{1} #{2} #{3}})))

    (let [graph {0 #{1} 1 #{0}
                 2 #{}  3 #{2 1}}]
      (is (= (tarjan graph)
             #{#{0 1} #{2} #{3}})))

    ;; FIXME Busted case
    (let [graph {1 #{7 3 5} 3 #{7 5}
                 5 #{}      7 #{3 5}}]
      (is (= (tarjan graph)
             #{#{1} #{3 7} #{5}}))))

  (testing "can check a one node graph"
    (let [graph {0 #{}}]
      (is (= (tarjan graph)
             #{#{0}})))))

(deftest busted-test
  #_(let [graph {1 #{7 3 5} 3 #{7 5}
               5 #{}      7 #{3 5}}]
    (is (= (tarjan graph)
           #{#{1} #{3 7} #{5}})))

  ;; WIki
  (let [graph {1 #{2}   2 #{3}
               3 #{1}   4 #{2 3 5}
               5 #{4 6} 6 #{3 7}
               7 #{6}   8 #{7 8}}]
    (is (= (tarjan graph)
           #{#{3 2 1} #{6 7} #{5 4} #{8}})))
  )

(deftest key-index-test
  (testing "Can produce a key-index from a history"
    (let [history [{:index 0 :type :ok, :f :read, :value {:x 0, :y 0}}
                   {:index 2 :type :ok, :f :read, :value {:x 1, :y 0}}
                   {:index 4 :type :ok, :f :read, :value {:x 1, :y 1}}
                   {:index 5 :type :ok, :f :read, :value {:x 0, :y 1}}]
          ki (reduce key-index (sorted-map) history)]
      (is (= ki
             {:x {0 #{0 5}
                  1 #{2 4}}
              :y {0 #{0 2}
                  1 #{4 5}}})))))

(deftest key-orders-test
  (testing "Can produce a map of keys to their successors"
    (let [history [{:index 0 :type :ok, :f :read, :value {:x 0, :y 0}}
                   {:index 2 :type :ok, :f :read, :value {:x 1, :y 0}}
                   {:index 4 :type :ok, :f :read, :value {:x 1, :y 1}}
                   {:index 5 :type :ok, :f :read, :value {:x 0, :y 1}}]
          ki {:x {0 #{0 5},
                  1 #{4 2}},
              :y {0 #{0 2},
                  1 #{4 5}}}
          ko (reduce (partial key-orders ki) {} history)]
      (is (= ko
             {:x {0 #{2 4}
                  2 #{}
                  4 #{}
                  5 #{2 4}}
              :y {0 #{4 5}
                  2 #{4 5}
                  4 #{}
                  5 #{}}}))))

  (testing "Can bridge missing values"
    (let [history [{:index 0 :type :ok, :f :read, :value {:x 0, :y 0}}
                   {:index 2 :type :ok, :f :read, :value {:x 1, :y 1}}
                   {:index 4 :type :ok, :f :read, :value {:x 4, :y 1}}
                   {:index 5 :type :ok, :f :read, :value {:x 0, :y 1}}]
          ki {:x {0 #{0 5}
                  1 #{2}
                  4 #{4}}
              :y {0 #{0}
                  1 #{2 4 5}}}
          ko (reduce (partial key-orders ki) {} history)]
      (is (= ko
             {:x {0 #{2}
                  2 #{4}
                  4 #{}
                  5 #{2}}
              :y {0 #{4 2 5}
                  2 #{}
                  4 #{}
                  5 #{}}})))))

(deftest precedence-graph-test
  (testing "can build a precedence graph from a history"
    (let [history [{:index 0 :type :ok, :f :read, :value {:x 0, :y 0}}
                   {:index 2 :type :ok, :f :read, :value {:x 1, :y 0}}
                   {:index 4 :type :ok, :f :read, :value {:x 1, :y 1}}
                   {:index 5 :type :ok, :f :read, :value {:x 0, :y 1}}]
          graph (precedence-graph history)]
      (is (= graph
             {0 #{2 4 5},
              2 #{4 5},
              4 #{},
              5 #{2 4}})))))

(defn big-history-gen
  [v]
  (let [f    (rand-nth [:inc :read])
        proc (rand-int 100)
        k    (rand-nth [[:x] [:y] [:x :y]])
        type (rand-nth [:ok :ok :ok :ok :ok
                        :fail :info :info])]
    [{:process proc, :type :invoke, :f f, :value {k v}}
     {:process proc, :type type,    :f f, :value {k v}}]))

(deftest checker-test
  #_(testing "Can validate histories"
    (let [checker (checker)
          history [{:index 0 :type :invoke :process 0 :f :read :value nil}
                   {:index 1 :type :ok     :process 0 :f :read :value {:x 0 :y 0}}
                   {:index 2 :type :invoke :process 0 :f :inc :value [:x]}
                   {:index 3 :type :ok     :process 0 :f :inc :value [:y]}
                   {:index 4 :type :invoke :process 0 :f :read :value nil}
                   {:index 5 :type :ok     :process 0 :f :read :value {:x 1 :y 1}}]
          r (checker/check checker nil history nil)]
      (is (:valid? r))))

  (testing "Can invalidate histories"
    (let [checker (checker)
          history [{:index 0 :type :invoke :process 0 :f :read :value nil}
                   {:index 1 :type :ok     :process 0 :f :read :value {:x 0 :y 0}}
                   {:index 2 :type :invoke :process 0 :f :read :value nil}
                   {:index 3 :type :ok     :process 0 :f :read :value {:x 1 :y 0}}
                   {:index 4 :type :invoke :process 0 :f :read :value nil}
                   {:index 5 :type :ok     :process 0 :f :read :value {:x 1 :y 1}}
                   {:index 6 :type :invoke :process 0 :f :read :value nil}
                   {:index 7 :type :ok     :process 0 :f :read :value {:x 0 :y 1}}]
          r (checker/check checker nil history nil)]
      (is (not (:valid? r)))))

  #_(testing "Can handle large histories"
    (let [checker (checker)
          history (->> (range)
                       (mapcat big-history-gen)
                       (take 10000)
                       vec)
          history  (map-indexed #(assoc %2 :index %1) history)
          r (checker/check checker nil history nil)]
      (is (:valid? r)))))

(defn read-only-gen
  [v]
  (let [proc (rand-int 100)]
    [{:process proc, :type :ok, :f :read, :value {:x v :y v}}]))

(deftest ^:integration stackoverflow-test
  (testing "just inducing the depth limit problem"
    (let [checker (checker)
          history (->> (range)
                       (mapcat read-only-gen)
                       (take 1000000)
                       (map-indexed #(assoc %2 :index %1))
                       vec)]
      (time
       (dotimes [n 1]
         (print "Run" n ":")
         (time (let [r (checker/check checker nil history nil)]
                 (is (:valid? r)))))))))
