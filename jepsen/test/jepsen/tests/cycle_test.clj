(ns jepsen.tests.cycle-test
  (:require [jepsen.tests.cycle :refer :all]
            [jepsen.checker :as checker]
            [jepsen.util :refer [map-vals]]
            [jepsen.txn :as txn]
            [knossos [history :as history]
                     [op :as op]]
            [clojure.test :refer :all]
            [fipp.edn :refer [pprint]]))

(deftest tarjan-test
  (let [tarjan (comp set tarjan)]
    (testing "Can analyze integer graphs"
      ;; From wikipedia
      (let [graph {1 #{2}   2 #{3}
                   3 #{1}   4 #{2 3 5}
                   5 #{4 6} 6 #{3 7}
                   7 #{6}   8 #{7 8}}]
        (is (= (tarjan graph)
               #{#{3 2 1} #{6 7} #{5 4}})))

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
               #{}))))

    (testing "Can flag self-ref as solo sccs"
      (let [graph {1 #{1} 2 #{2}
                   3 #{3} 4 #{4}}]
        (is (= (tarjan graph)
               #{}))))

    (testing "can check monotonic loop histories"
      ;; Linear
      (let [graph {0 #{1} 1 #{2}
                   2 #{3} 3 #{}}]
        (is (= (tarjan graph)
               #{})))

      ;; Loop
      (let [graph {0 #{1} 1 #{2}
                   2 #{1} 3 #{}}]
        (is (= (tarjan graph)
               #{#{1 2}})))

      ;; Linear but previously bugged case
      (let [graph {0 #{1} 1 #{2}
                   2 #{}  3 #{2 1}}]
        (is (= (tarjan graph)
               #{})))

      (let [graph {0 #{1} 1 #{0}
                   2 #{}  3 #{2 1}}]
        (is (= (tarjan graph)
               #{#{0 1}})))

      ;; FIXME Busted case
      (let [graph {1 #{7 3 5} 3 #{7 5}
                   5 #{}      7 #{3 5}}]
        (is (= (tarjan graph)
               #{#{3 7}}))))

    (testing "can check a one node graph"
      (let [graph {0 #{}}]
        (is (= (tarjan graph)
               #{}))))

    (testing "busted"
      (let [graph {1 #{7 3 5} 3 #{7 5}
                   5 #{}      7 #{3 5}}]
        (is (= (tarjan graph)
               #{#{3 7}}))))

    (testing "wiki"
      (let [graph {1 #{2}   2 #{3}
                   3 #{1}   4 #{2 3 5}
                   5 #{4 6} 6 #{3 7}
                   7 #{6}   8 #{7 8}}]
        (is (= (tarjan graph)
               #{#{3 2 1} #{6 7} #{5 4}}))))))

(deftest process-graph-test
  (let [o1 {:index 0 :process 1 :type :ok}
        o2 {:index 1 :process 2 :type :ok}
        o3 {:index 2 :process 2 :type :ok}
        o4 {:index 3 :process 1 :type :ok}
        history [o1 o2 o3 o4]]
    (is (= {[:process 1] {o1 #{o4}, o4 #{}}
            [:process 2] {o2 #{o3}, o3 #{}}}
           (->clj (process-graph history))))))

(deftest monotonic-key-graph-test
  (testing "basics"
    (let [r1 {:index 0 :type :ok, :f :read, :value {:x 0, :y 0}}
          r2 {:index 2 :type :ok, :f :read, :value {:x 1, :y 0}}
          r3 {:index 4 :type :ok, :f :read, :value {:x 1, :y 1}}
          r4 {:index 5 :type :ok, :f :read, :value {:x 0, :y 1}}
          history [r1 r2 r3 r4]]
      (is (= {[:key :x] {r1 #{r2 r3}, r2 #{},      r3 #{}, r4 #{r2 r3}}
              [:key :y] {r1 #{r3 r4}, r2 #{r3 r4}, r3 #{}, r4 #{}}}
             (->clj (monotonic-key-graph history))))))

  (testing "Can bridge missing values"
    (let [r1 {:index 0 :type :ok, :f :read, :value {:x 0, :y 0}}
          r2 {:index 2 :type :ok, :f :read, :value {:x 1, :y 1}}
          r3 {:index 4 :type :ok, :f :read, :value {:x 4, :y 1}}
          r4 {:index 5 :type :ok, :f :read, :value {:x 0, :y 1}}
          history [r1 r2 r3 r4]]
      (is (= {[:key :x] {r1 #{r2},       r2 #{r3}, r3 #{}, r4 #{r2}}
              [:key :y] {r1 #{r2 r3 r4}, r2 #{}, r3 #{}, r4 #{}}}
             (->clj (monotonic-key-graph history)))))))

(deftest full-graph-test
  (let [r1 {:index 0 :type :ok, :f :read, :value {:x 0, :y 0}}
        r2 {:index 2 :type :ok, :f :read, :value {:x 1, :y 0}}
        r3 {:index 4 :type :ok, :f :read, :value {:x 1, :y 1}}
        r4 {:index 5 :type :ok, :f :read, :value {:x 0, :y 1}}
        history [r1 r2 r3 r4]
        orders (monotonic-key-graph history)]
    (is (= {r1 #{r2 r3 r4}
            r2 #{r3 r4}
            r3 #{}
            r4 #{r2 r3}}
           (->clj (full-graph orders))))))

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
  (testing "valid"
    (let [checker (checker monotonic-key-graph)
          history [{:index 0 :type :invoke :process 0 :f :read :value nil}
                   {:index 1 :type :ok     :process 0 :f :read :value {:x 0 :y 0}}
                   {:index 2 :type :invoke :process 0 :f :inc :value [:x]}
                   {:index 3 :type :ok     :process 0 :f :inc :value {:x 1}}
                   {:index 4 :type :invoke :process 0 :f :read :value nil}
                   {:index 5 :type :ok     :process 0 :f :read :value {:x 1 :y 1}}]]
      (is (= {:valid? true
              :cycles []}
             (checker/check checker nil history nil)))))

  (testing "invalid"
    (let [checker (checker monotonic-key-graph)
          r00  {:index 0 :type :invoke :process 0 :f :read :value nil}
          r00' {:index 1 :type :ok     :process 0 :f :read :value {:x 0 :y 0}}
          r10  {:index 2 :type :invoke :process 0 :f :read :value nil}
          r10' {:index 3 :type :ok     :process 0 :f :read :value {:x 1 :y 0}}
          r11  {:index 4 :type :invoke :process 0 :f :read :value nil}
          r11' {:index 5 :type :ok     :process 0 :f :read :value {:x 1 :y 1}}
          r01  {:index 6 :type :invoke :process 0 :f :read :value nil}
          r01' {:index 7 :type :ok     :process 0 :f :read :value {:x 0 :y 1}}
          history [r00 r00' r10 r10' r11 r11' r01 r01']]
      (is (= {:valid? false
              :cycles [[r01' [:key :x] r10' [:key :y] r01']]}
             (checker/check checker nil history nil)))))

  (testing "large histories"
    (let [checker (checker monotonic-key-graph)
          history (->> (range)
                       (mapcat big-history-gen)
                       (take 10000)
                       vec)
          history  (map-indexed #(assoc %2 :index %1) history)
          r (checker/check checker nil history nil)]
      (is (:valid? r)))))

(deftest monotonic+process-test
  ; Here, we construct an order which is legal on keys AND is sequentially
  ; consistent, but the key order is incompatible with process order.
  (let [r1 {:type :ok, :process 0, :f :read, :value {:x 1}}
        r2 {:type :ok, :process 0, :f :read, :value {:x 0}}
        history [r1 r2]]
    (testing "combined order"
      (is (= {[:key :x]     {r2 #{r1}, r1 #{}}
              [:process 0]  {r1 #{r2}, r2 #{}}}
             (->clj ((combine monotonic-key-graph process-graph) history)))))
    (testing "independently valid"
      (is (= {:valid? true
              :cycles []}
             (checker/check (checker monotonic-key-graph) nil history nil)))
      (is (= {:valid? true
              :cycles []}
             (checker/check (checker process-graph) nil history nil))))
    (testing "combined invalid"
      (is (= {:valid? false
              :cycles [[r1 [:process 0] r2 [:key :x] r1]]}
             (checker/check (checker (combine monotonic-key-graph
                                              process-graph))
                            nil history nil))))))

(defn read-only-gen
  [v]
  (let [proc (rand-int 100)]
    [{:process proc, :type :ok, :f :read, :value {:x v :y v}}]))

(deftest ^:integration stackoverflow-test
  (testing "just inducing the depth limit problem"
    (let [checker (checker monotonic-key-graph)
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

(deftest ext-index-test
  (testing "empty"
    (is (= {} (ext-index txn/ext-writes []))))
  (testing "writes"
    (let [w1 {:type :ok, :value [[:w :x 1] [:w :y 3] [:w :x 2]]}
          w2 {:type :ok, :value [[:w :y 3] [:w :x 4]]}]
      (is (= {:x {2 [w1], 4 [w2]}
              :y {3 [w2 w1]}}
             (ext-index txn/ext-writes [w1 w2]))))))

(deftest wr-graph-test
  ; helper fns for constructing histories
  (let [op (fn [txn] [{:type :invoke, :f :txn, :value txn}
                      {:type :ok,     :f :txn, :value txn}])
        check (fn [& txns]
                (let [h (mapcat op txns)]
                  (checker/check (checker wr-graph) nil h nil)))]
    (testing "empty history"
      (is (= {:valid? true, :cycles []}
             (check []))))
    (testing "write and read"
      (is (= {:valid? true, :cycles []}
             (check [[:w :x 0]]
                    [[:w :x 0]]))))
    (testing "chain on one register"
      (is (false? (:valid? (check [[:r :x 0] [:w :x 1]]
                                  [[:r :x 1] [:w :x 0]])))))
    (testing "chain across two registers"
      (is (false? (:valid? (check [[:r :x 0] [:w :y 1]]
                                  [[:r :y 1] [:w :x 0]])))))
    (testing "write skew"
      ; this violates si, but doesn't introduce a w-r conflict, so it's legal
      ; as far as this order is concerned.
      (is (true? (:valid? (check [[:r :x 0] [:r :y 0] [:w :x 1]]
                                 [[:r :x 0] [:r :y 0] [:w :y 1]])))))))

(deftest realtime-graph-test
  ; We're gonna try a bunch of permutations of diff orders, so we'll index,
  ; analyze, then remove indices, to simplify comparison. This is safe because
  ; all ops are unique without indices.
  (let [o (fn [& ops] (->> (history/index ops)
                           realtime-graph
                           :realtime
                           ->clj
                           (map (fn [[k vs]]
                                  [(dissoc k :index)
                                   (map #(dissoc % :index) vs)]))
                           (into {})))
        a  {:type :invoke, :process 1, :f :read, :value nil}
        a' {:type :ok      :process 1, :f :read, :value 1}
        b  {:type :invoke, :process 2, :f :read, :value nil}
        b' {:type :ok      :process 2, :f :read, :value 2}
        c  {:type :invoke, :process 3, :f :read, :value nil}
        c' {:type :ok      :process 3, :f :read, :value 3}
        d  {:type :invoke, :process 4, :f :read, :value nil}
        d' {:type :ok      :process 4, :f :read, :value 4}
        e  {:type :invoke, :process 5, :f :read, :value nil}
        e' {:type :ok      :process 5, :f :read, :value 5}]
    (testing "empty history"
      (is (= {} (o))))
    (testing "single op"
      (is (= {} (o a a'))))
    (testing "two sequential ops"
      (is (= {a' [b'], b' []}
             (o a a' b b'))))
    (testing "three ops in a row"
      (is (= {a' [b'], b' [c'], c' []}
             (o a a' b b' c c'))))
    (testing "one followed by two concurrent"
      (is (= {a' [b' c'], b' [], c' []}
             (o a a' b c c' b'))))
    (testing "two concurrent followed by one"
      (is (= {a' [c'], b' [c'], c' []}
             (o a b a' b' c c'))))
    (testing "two concurrent followed by two concurrent"
      (is (= {a' [d' c'], b' [d' c'], c' [], d' []}
             (o a b b' a' c d c' d'))))
    (testing "complex"
      ;   ==a==       ==c== ==e==
      ;         ==b==
      ;           ==d===
      ;
      (is (= {a' [b' d'], b' [c'], c' [e'], d' [e'], e' []}
             (o a a' b d b' c d' c' e e'))))))
