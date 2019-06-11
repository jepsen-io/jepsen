(ns jepsen.tests.cycle-test
  (:require [jepsen.tests.cycle :refer :all]
            [jepsen.checker :as checker]
            [jepsen.util :refer [map-vals spy]]
            [jepsen.txn :as txn]
            [knossos [history :as history]
                     [op :as op]]
            [clojure.test :refer :all]
            [fipp.edn :refer [pprint]]
            [slingshot.slingshot :refer [try+ throw+]]))

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
    (is (= {o1 #{o4}, o2 #{o3}, o3 #{}, o4 #{}}
           (->clj (first (process-graph history)))))))

(deftest monotonic-key-graph-test
  (testing "basics"
    (let [r1 {:index 0 :type :ok, :f :read, :value {:x 0, :y 0}}
          r2 {:index 2 :type :ok, :f :read, :value {:x 1, :y 0}}
          r3 {:index 4 :type :ok, :f :read, :value {:x 1, :y 1}}
          r4 {:index 5 :type :ok, :f :read, :value {:x 0, :y 1}}
          history [r1 r2 r3 r4]]
      (is (= {r1 #{r2 r3 r4}
              r2 #{r3 r4}
              r3 #{}
              r4 #{r2 r3}}
             (->clj (first (monotonic-key-graph history)))))))

  (testing "Can bridge missing values"
    (let [r1 {:index 0 :type :ok, :f :read, :value {:x 0, :y 0}}
          r2 {:index 2 :type :ok, :f :read, :value {:x 1, :y 1}}
          r3 {:index 4 :type :ok, :f :read, :value {:x 4, :y 1}}
          r4 {:index 5 :type :ok, :f :read, :value {:x 0, :y 1}}
          history [r1 r2 r3 r4]]
      (is (= {r1 #{r2 r3 r4}
              r2 #{r3}
              r3 #{}
              r4 #{r2}}
             (->clj (first (monotonic-key-graph history))))))))

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
      (is (= {:valid?     true
              :scc-count  0
              :cycles     []}
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
              :scc-count 1
              :cycles ["Let:\n  T1 = {:index 7, :type :ok, :process 0, :f :read, :value {:x 0, :y 1}}\n  T2 = {:index 3, :type :ok, :process 0, :f :read, :value {:x 1, :y 0}}\n\nThen:\n  - T1 < T2, because T1 observed :x = 0, and T2 observed a higher value 1.\n  - However, T2 < T1, because T2 observed :y = 0, and T1 observed a higher value 1: a contradiction!"]}
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
  (let [[r1 r2 :as history]
        (history/index [{:type :ok, :process 0, :f :read, :value {:x 1}}
                        {:type :ok, :process 0, :f :read, :value {:x 0}}])]
    (testing "combined order"
      (let [[graph explainer] ((combine monotonic-key-graph process-graph)
                               history)]
        (is (= {r1 #{r2} r2 #{r1}}
               (->clj graph)))))
    (testing "independently valid"
      (is (= {:valid? true
              :scc-count 0
              :cycles []}
             (checker/check (checker monotonic-key-graph) nil history nil)))
      (is (= {:valid? true
              :scc-count 0
              :cycles []}
             (checker/check (checker process-graph) nil history nil))))
    (testing "combined invalid"
      (is (= {:valid? false
              :scc-count 1
              :cycles ["Let:\n  T1 = {:type :ok, :process 0, :f :read, :value {:x 0}, :index 1}\n  T2 = {:type :ok, :process 0, :f :read, :value {:x 1}, :index 0}\n\nThen:\n  - T1 < T2, because T1 observed :x = 0, and T2 observed a higher value 1.\n  - However, T2 < T1, because process 0 executed T2 before T1: a contradiction!"]}
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
      (is (= {:valid? true, :scc-count 0, :cycles []}
             (check []))))
    (testing "write and read"
      (is (= {:valid? true, :scc-count 0, :cycles []}
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

(defn graph
  "Takes a history, indexes it, uses the given analyzer function to construct a
  graph+explainer, extracts just the graph, converts it to Clojure, and removes
  indices from the ops."
  [analyzer history]
  (->> history
       history/index
       analyzer
       first
       ->clj
       (map (fn [[k vs]]
              [(dissoc k :index)
               (map #(dissoc % :index) vs)]))
       (into {})))

(deftest realtime-graph-test
  ; We're gonna try a bunch of permutations of diff orders, so we'll index,
  ; analyze, then remove indices, to simplify comparison. This is safe because
  ; all ops are unique without indices.
  (let [o (comp (partial graph realtime-graph) vector)
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

(deftest path-shells-test
  (let [g     (map->bdigraph {0 [1 2] 1 [3] 2 [3] 3 [0]})
        paths (path-shells g [[0]])]
    (is (= [[[0]]
            [[0 1] [0 2]]
            [[0 1 3]]
            [[0 1 3 0]]]
           (take 4 paths)))))

(deftest find-cycle-test
  (let [g (map->bdigraph {0 [1 2]
                          1 [4]
                          2 [3]
                          3 [4]
                          4 [0 2]})]
    (testing "basic cycle"
      (is (= [0 1 4 0]
             (find-cycle g (->clj (.vertices g))))))

    ; We may restrict a graph to a particular relationship and look for cycles
    ; in an SCC found in a larger graph; this should still work.
    (testing "scc without cycle in graph"
      (is (= nil
             (find-cycle g #{0 2 4}))))

    (testing "cycle in restricted scc"
      (is (= [0 1 4 0]
             (find-cycle g #{0 1 4}))))))

(deftest find-cycle-starting-with-test
  (let [initial   (map->bdigraph {0 [1 2]})
        ; Remaining HAS a cycle, but we don't want to find it.
        remaining (map->bdigraph {1 [3]
                                  3 [1 0]})]
    (testing "without 0"
      (is (= nil (find-cycle-starting-with initial remaining #{1 2 3}))))
    (testing "with 0"
      (is (= [0 1 3 0]
             (find-cycle-starting-with initial remaining #{0 1 2 3}))))))

(deftest renumber-graph-test
  (is (= [{} []]
         (update (renumber-graph (map->bdigraph {})) 0 ->clj)))
  (is (= [{0 #{1 3}
           1 #{0}
           2 #{0}
           3 #{}}
          [:y :t :x :z]]
         (update (renumber-graph (map->bdigraph {:x #{:y}
                                                 :y #{:z :t}
                                                 :z #{}
                                                 :t #{:y}}))
                 0 ->clj))))

(deftest link-test
  (let [g (-> (directed-graph)
              (link 1 2 :foo)
              (link 1 2 :bar))]
    (is (= #{:foo :bar} (edge g 1 2)))))

(deftest project+remove-relationship-test
  (let [g (-> (directed-graph)
              (link 1 2 :foo)
              (link 1 3 :foo)
              (link 2 3 :bar)
              (link 1 2 :bar))]
    (testing "remove"
      (is (= #{{:from 1, :to 2, :value #{:bar}}
               {:from 2, :to 3, :value #{:bar}}}
             (set (map ->clj (edges (remove-relationship g :foo)))))))
    (testing "project"
      (is (= #{{:from 1, :to 2, :value #{:foo}}
               {:from 1, :to 3, :value #{:foo}}}
             (set (map ->clj (edges (project-relationship g :foo)))))))))
