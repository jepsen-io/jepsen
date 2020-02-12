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
  ([string]
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
  ([process type string]
   (assoc (op string) :process process :type type)))

(defn fail
  "Fails an op."
  [op]
  (assoc op :type :fail))

(defn pair
  "Takes a completed op and returns an [invocation, completion] pair."
  [completion]
  [(-> completion
       (assoc :type :invoke)
       (update :value (partial map (fn [[f k v :as mop]]
                                        (if (= :r f)
                                          [f k nil]
                                          mop)))))
   completion])

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

(deftest transaction-graph->version-graphs-test
  ; Turn transaction graphs (in clojure maps) into digraphs, then into version
  ; graphs, then back into clojure.
  (let [vg (fn [txn-graph]
             (-> txn-graph
                 cycle/map->bdigraph
                 transaction-graph->version-graphs
                 cycle/->clj))]
    (testing "empty"
      (is (= {}
             (vg {}))))

    (testing "r->w"
      (is (= {:x {1 #{2}
                  2 #{}}}
             (vg {(op "rx1") [(op "wx2")]}))))

    (testing "fork-join"
      (is (= {:x {1 #{2 3}
                  2 #{4}
                  3 #{4}
                  4 #{}}}
             (vg {(op "rx1") [(op "wx2") (op "wx3")]
                  (op "wx2") [(op "rx4")]
                  (op "wx3") [(op "rx4")]}))))

    (testing "external ww"
      (is (= {:x {2 #{4}, 4 #{}}}
             ; 3 is an internal version; we want to generate 2->4!
             (vg {(op "wx1wx2") [(op "wx3wx4rx5")]}))))

    (testing "external wr"
      (is (= {:x {2 #{3}, 3 #{}}}
             (vg {(op "wx1wx2") [(op "rx3rx4wx5")]}))))

    (testing "external rw"
      (is (= {:x {1 #{4}, 4 #{}}}
             (vg {(op "rx1rx2") [(op "wx3wx4rx5")]}))))

    (testing "external rr"
      (is (= {:x {1 #{3}, 3 #{}}}
             (vg {(op "rx1rx2") [(op "rx3rx4wx5")]}))))

    (testing "don't infer v1 -> v1 deps"
      (is (= {:x {}}
             (vg {(op "wx1") [(op "rx1")]}))))))

(deftest version-graphs->transaction-graphs-test
  (testing "empty"
    (is (= {}
           (cycle/->clj (version-graphs->transaction-graph
                          []
                          (cycle/digraph))))))
  (testing "rr"
    ; We don't generate rr edges, under the assumption they'll be covered by
    ; rw/wr/ww edges.
    (is (= {}
           (->> {:x (cycle/map->bdigraph {1 [2], 2 [3]})}
                (version-graphs->transaction-graph [(op "rx1") (op "rx2")])
                cycle/->clj))))

  (testing "wr"
    ; We don't emit wr edges here--wr-graph does that for us. Maybe we should
    ; later? Wouldn't be hard...
    (is (= {}
           (->> {:x (cycle/map->bdigraph {1 [2], 2 [3]})}
                (version-graphs->transaction-graph [(op "wx1") (op "rx1")])
                cycle/->clj))))

  (testing "ww"
    (is (= {(op "wx1") #{(op "wx2")}
            (op "wx2") #{}}
           (->> {:x (cycle/map->bdigraph {1 [2], 2 [3]})}
                (version-graphs->transaction-graph [(op "wx1") (op "wx2")])
                cycle/->clj))))

  (testing "rw"
    (is (= {(op "rx1") #{(op "wx2")}
            (op "wx2") #{}}
           (->> {:x (cycle/map->bdigraph {1 [2], 2 [3]})}
                (version-graphs->transaction-graph [(op "rx1") (op "wx2")])
                cycle/->clj))))

  (testing "ignores internal writes/reads"
    (is (= {}
           (->> {:x (cycle/map->bdigraph {1 [2], 2 [3]})}
                (version-graphs->transaction-graph [(op "wx1wx2")
                                                    (op "rx2rx3")])
                cycle/->clj)))))

(deftest checker-test
  (let [c (fn [checker-opts history]
            (checker/check (checker checker-opts) nil
                           (history/index history) nil))]

    (testing "G0"
      ; What (could be) a pure write cycle: T1 < T2 on x, T2 < T1 on y.
      (let [[t1 t1'] (pair (op 0 :ok "wx1wy2"))
            [t2 t2'] (pair (op 1 :ok "wx2wy1"))]
        ; Of course we can't detect this naively: there's no wr-cycle, and we
        ; can't say anything about versions. This *isn't* illegal yet!
        (is (= {:valid?         :unknown
                :anomaly-types  [:empty-transaction-graph]
                :anomalies      {:empty-transaction-graph true}}
               (c {:anomalies [:G0]} [t1 t2])))

        ; But let's say we observe a read *after* both transactions which shows
        ; that the final value of x and y are both 2? We can infer this from
        ; sequential keys alone, as long as the version order aligns.
        (let [[t3 t3'] (pair (op 0 :ok "rx2"))
              [t4 t4'] (pair (op 1 :ok "ry2"))]
          (is (= {:valid?         false
                  :anomaly-types  [:G0]
                  :anomalies      {:G0 ["Let:\n  T1 = {:type :ok, :value [[:w :x 1] [:w :y 2]], :process 0, :index 1}\n  T2 = {:type :ok, :value [[:w :x 2] [:w :y 1]], :process 1, :index 3}\n\nThen:\n  - T1 < T2, because T1 set key :x to 1, and T2 set it to 2, which came later in the version order.\n  - However, T2 < T1, because T2 set key :y to 1, and T1 set it to 2, which came later in the version order: a contradiction!"]}}
                 (c {:anomalies         [:G0]
                     :sequential-keys?  true}
                    [t1 t1' t2 t2' t3 t3' t4 t4']))))))

    (testing "G1a"
      (let [; T2 sees T1's failed write
            t1 (fail (op "wx1"))
            t2 (op "rx1")]
        (is (= {:valid? false
                :anomaly-types [:G1a :empty-transaction-graph]
                :anomalies {:empty-transaction-graph true
                            :G1a [{:op      (assoc t2 :index 0)
                                   :writer  (assoc t1 :index 1)
                                   :mop     [:r :x 1]}]}}
               (c {:anomalies [:G1]} [t2 t1])))))

    (testing "G1b"
      (let [; T2 sees T1's intermediate write
            t1 (op "wx1wx2")
            t2 (op "rx1")
            h  [t1 t2]]
        ; G0 checker won't catch this
        (is (= {:valid? :unknown
                :anomaly-types [:empty-transaction-graph]
                :anomalies {:empty-transaction-graph true}}
               (c {:anomalies [:G0]} h)))

        ; G1 will
        (is (= {:valid? false
                :anomaly-types [:G1b :empty-transaction-graph]
                :anomalies {:empty-transaction-graph true
                            :G1b [{:op      (assoc t2 :index 1)
                                   :writer  (assoc t1 :index 0)
                                   :mop     [:r :x 1]}]}}
               (c {:anomalies [:G1]} h)))))

    (testing "G1c"
      (let [; T2 observes T1's write of x, and vice versa on y.
            t1 (op "wx1ry1")
            t2 (op "wy1rx1")
            h  [t1 t2]
            msg "Let:\n  T1 = {:type :ok, :value [[:w :y 1] [:r :x 1]], :index 1}\n  T2 = {:type :ok, :value [[:w :x 1] [:r :y 1]], :index 0}\n\nThen:\n  - T1 < T2, because T1 wrote :y = 1, which was read by T2.\n  - However, T2 < T1, because T2 wrote :x = 1, which was read by T1: a contradiction!"]
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

    (testing "G2"
      (let [[t1 t1'] (pair (op 0 :ok "rx1ry1"))  ; Establish the initial state
            [t2 t2'] (pair (op 1 :ok "rx1wy2"))  ; Advance y
            [t3 t3'] (pair (op 2 :ok "ry1wx2"))] ; Advance x
        ; G2 should catch this, so long as we can use the linearizable key
        ; assumption to infer that t2 and t3's writes of 2 follow
        ; the initial states of 1.
        (is (= {:valid? false
                :anomaly-types [:G2]
                :anomalies {:G2 ["Let:\n  T1 = {:type :ok, :value [[:r :x 1] [:w :y 2]], :process 1, :index 5}\n  T2 = {:type :ok, :value [[:r :y 1] [:w :x 2]], :process 2, :index 4}\n\nThen:\n  - T1 < T2, because T1 read key :x = 1, and T2 set it to 2, which came later in the version order.\n  - However, T2 < T1, because T2 read key :y = 1, and T1 set it to 2, which came later in the version order: a contradiction!"]}}
               (c {:anomalies         [:G2]
                   :linearizable-keys? true}
                  [t1 t1' t2 t3 t3' t2'])))))

    (testing "internal"
      (let [t1 (op "rx1rx2")
            h  [t1]]
        (is (= {:valid? false
                :anomaly-types [:empty-transaction-graph :internal]
                :anomalies {:internal [{:op       (assoc t1 :index 0)
                                        :mop      [:r :x 2]
                                        :expected 1}]
                            :empty-transaction-graph true}}
               (c {:anomalies [:internal]} h)))))

    (testing "initial state"
      (let [[t1 t1'] (pair (op 0 :ok "rx_ry1"))
            [t2 t2'] (pair (op 0 :ok "wy1wx2"))]
        ; We can infer, on the basis that nil *must* precede every non-nil
        ; value, plus the direct wr dep, that this constitutes a G-single
        ; anomaly!
        (is (= {:valid? false
                :anomaly-types [:G-single]
                :anomalies {:G-single ["Let:\n  T1 = {:type :ok, :value [[:r :x nil] [:r :y 1]], :process 0, :index 3}\n  T2 = {:type :ok, :value [[:w :y 1] [:w :x 2]], :process 0, :index 2}\n\nThen:\n  - T1 < T2, because T1 read key :x = nil, and T2 set it to 2, which came later in the version order.\n  - However, T2 < T1, because T2 wrote :y = 1, which was read by T1: a contradiction!"]}}
               (c {} [t1 t2 t2' t1'])))))

    (testing "wfr"
      (let [t1 (op 0 :ok "ry1wx1wy2")  ; Establishes y: 1 -> 2
            t2 (op 0 :ok "rx1ry1")]    ; t1 <wr t2, on x, but also t2 <rw t1, on y!
        ; We can't see this without knowing the version order on y
        (is (= {:valid? true}
               (c {:wfr-keys? false} [t1 t2])))
        ; But if we use WFR, we know 1 < 2, and can see the G-single
        (is (= {:valid? false
                :anomaly-types [:G-single]
                :anomalies {:G-single ["Let:\n  T1 = {:type :ok, :value [[:r :x 1] [:r :y 1]], :process 0, :index 1}\n  T2 = {:type :ok, :value [[:r :y 1] [:w :x 1] [:w :y 2]], :process 0, :index 0}\n\nThen:\n  - T1 < T2, because T1 read key :y = 1, and T2 set it to 2, which came later in the version order.\n  - However, T2 < T1, because T2 wrote :x = 1, which was read by T1: a contradiction!"]}}
               (c {:wfr-keys? true} [t1 t2])))))

    (testing "cyclic version order"
      (let [[t1 t1'] (pair (op 0 :ok "wx1"))
            [t2 t2'] (pair (op 0 :ok "wx2"))
            [t3 t3'] (pair (op 0 :ok "rx1"))]
        (is (= {:valid?         false
                :anomaly-types  [:cyclic-versions]
                :anomalies      {:cyclic-versions
                                 [{:key :x, :scc #{1 2} :sources [:initial-state
                                                                  :sequential-keys]}]}}
               (c {:sequential-keys? true}
                  [t1 t1' t2 t2' t3 t3'])))))))

(def dgraph-history
[
])

(deftest dgraph-example
;  (is (= {:valid? true}
;         (checker/check (checker {:additional-graphs [cycle/realtime-graph]
;                                  :linearizable-keys? true})
;                        nil
;                        (history/index dgraph-history)
;                        nil)))
)
