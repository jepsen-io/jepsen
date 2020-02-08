(ns jepsen.tests.cycle.wr-test
  (:refer-clojure :exclude [test])
  (:require [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.wr :refer :all]
            [jepsen.checker :as checker]
            [jepsen.util :refer [map-vals spy]]
            [jepsen.txn :as txn]
            [knossos [history :as history]
                     [op :as op]]
            [clojure.test :refer :all]
            [fipp.edn :refer [pprint]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn op
  "Generates an operation from a string language like so:

  wx1       set x = 1
  ry1       read y = 1
  wx1wx2    set x=1, x=2"
  [string]
  (let [[txn mop] (reduce (fn [[txn [f k v :as mop]] c]
                            (case c
                              \w [(conj txn mop) [:w]]
                              \r [(conj txn mop) [:r]]
                              \x [txn (conj mop :x)]
                              \y [txn (conj mop :y)]
                              \z [txn (conj mop :z)]
                              (let [e (if (= \_ c)
                                        nil
                                        (Long/parseLong (str c)))]
                                [txn [f k e]])))
                          [[] nil]
                          string)
        txn (-> txn
                (subvec 1)
                (conj mop))]
    {:type :ok, :value txn}))

(defn fail
  "Fails an op."
  [op]
  (assoc op :type :fail))

(deftest op-test
  (is (= {:type :ok, :value [[:w :x 1] [:r :x 2]]}
         (op "wx1rx2"))))

(deftest ext-index-test
  (testing "empty"
    (is (= {} (ext-index txn/ext-writes []))))
  (testing "writes"
    (let [w1 {:type :ok, :value [[:w :x 1] [:w :y 3] [:w :x 2]]}
          w2 {:type :ok, :value [[:w :y 3] [:w :x 4]]}]
      (is (= {:x {2 [w1], 4 [w2]}
              :y {3 [w2 w1]}}
             (ext-index txn/ext-writes [w1 w2]))))))

(deftest internal-cases-test
  (testing "empty"
    (is (= nil (internal-cases []))))

  (testing "good"
    (is (= nil (internal-cases [(op "ry2wx2rx2wx1rx1")]))))

  (testing "stale"
    (let [stale (op "rx1wx2rx1")]
      (is (= [{:op stale, :mop [:r :x 1], :expected 2}]
             (internal-cases [stale]))))))

(deftest g1a-cases-test
  (testing "empty"
    (is (= nil (g1a-cases []))))

  (testing "good"
    (is (= nil (g1a-cases [(op "wx1")
                           (op "rx1")]))))

  (testing "bad"
    (let [w (fail (op "wx2"))
          r (op "rx2")]
      (is (= [{:op r, :writer, w :mop [:r :x 2]}]
             (g1a-cases [r w]))))))

(deftest g1b-cases-test
  (testing "empty"
    (is (= nil (g1b-cases []))))

  (testing "good"
    (is (= nil (g1b-cases [(op "wx1wx2")
                           (op "rx_rx2")]))))

  (testing "bad"
    (let [w (op "wx2wx1")
          r (op "rx2")]
      (is (= [{:op r, :writer, w :mop [:r :x 2]}]
             (g1b-cases [r w]))))))

(deftest wr-graph-test
  ; helper fns for constructing histories
  (let [op (fn [txn] [{:type :invoke, :f :txn, :value txn}
                      {:type :ok,     :f :txn, :value txn}])
        check (fn [& txns]
                (let [h (mapcat op txns)]
                  (checker/check (cycle/checker wr-graph) nil h nil)))]
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

(deftest checker-test
  (let [c (fn [checker-opts history]
            (checker/check (checker checker-opts) nil history nil))]
    (testing "G1a"
      (let [; T2 sees T1's failed write
            t1 (fail (op "wx1"))
            t2 (op "rx1")]
        (is (= {:valid? false
                :anomaly-types [:G1a]
                :anomalies {:G1a [{:op      t2
                                   :writer  t1
                                   :mop     [:r :x 1]}]}}
               (c {:anomalies [:G1]} [t2 t1])))))

    (testing "G1b"
      (let [; T2 sees T1's intermediate write
            t1 (op "wx1wx2")
            t2 (op "rx1")
            h  [t1 t2]]
        ; G0 checker won't catch this
        (is (= {:valid? true} (c {:anomalies [:G0]} h)))

        ; G1 will
        (is (= {:valid? false
                :anomaly-types [:G1b]
                :anomalies {:G1b [{:op      t2
                                   :writer  t1
                                   :mop     [:r :x 1]}]}}
               (c {:anomalies [:G1]} h)))))

    (testing "G1c"
      (let [; T2 observes T1's write of x, and vice versa on y.
            t1 (op "wx1ry1")
            t2 (op "wy1rx1")
            h  [t1 t2]
            msg "Let:\n  T1 = {:type :ok, :value [[:w :y 1] [:r :x 1]]}\n  T2 = {:type :ok, :value [[:w :x 1] [:r :y 1]]}\n\nThen:\n  - T1 < T2, because T1 wrote :y = 1, which was read by T2.\n  - However, T2 < T1, because T2 wrote :x = 1, which was read by T1: a contradiction!"]
        ; G0 won't see this
        (is (= {:valid? true} (c {:anomalies [:G0]} h)))
        ; But G1 will!
        (is (= {:valid? false
                :anomaly-types [:G1c]
                :anomalies {:G1c [msg]}}
               (c {:anomalies [:G1]} h)))
        ; As will G2
        (is (= {:valid? false
                :anomaly-types [:G1c]
                :anomalies {:G1c [msg]}}
               (c {:anomalies [:G2]} h)))))

    (testing "internal"
      (let [t1 (op "rx1rx2")
            h  [t1]]
        (is (= {:valid? false
                :anomaly-types [:internal]
                :anomalies {:internal [{:op       t1
                                        :mop      [:r :x 2]
                                        :expected 1}]}}
               (c {:anomalies [:internal]} h)))))))
