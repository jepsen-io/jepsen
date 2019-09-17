(ns jepsen.tests.cycle.append-test
  (:require [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :refer :all]
            [jepsen.checker :as checker]
            [jepsen.util :refer [map-vals spy]]
            [jepsen.txn :as txn]
            [knossos [history :as history]
                     [op :as op]]
            [clojure.test :refer :all]
            [fipp.edn :refer [pprint]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn just-graph
  "Takes a history, indexes it, uses the given analyzer function to construct a
  graph+explainer, extracts just the graph, converts it to Clojure, and removes
  indices from the ops."
  [analyzer history]
  (->> history
       history/index
       analyzer
       first
       cycle/->clj
       (map (fn [[k vs]]
              [(dissoc k :index)
               (map #(dissoc % :index) vs)]))
       (into {})))

(defn op
  "Generates an operation from a string language like so:

  ax1       append 1 to x
  ry12      read y = [1 2]
  ax1ax2    append 1 to x, append 2 to x"
  [string]
  (let [[txn mop] (reduce (fn [[txn [f k v :as mop]] c]
                            (case c
                              \a [(conj txn mop) [:append]]
                              \r [(conj txn mop) [:r]]
                              \x [txn (conj mop :x)]
                              \y [txn (conj mop :y)]
                              \z [txn (conj mop :z)]
                              (let [e (Long/parseLong (str c))]
                                [txn [f k (case f
                                            :append e
                                            :r      (conj (or v []) e))]])))
                          [[] nil]
                          string)
        txn (-> txn
                (subvec 1)
                (conj mop))]
    {:type :ok, :value txn}))

(deftest op-test
  (is (= {:type :ok, :value [[:append :x 1] [:append :x 2]]}
         (op "ax1ax2"))))

(deftest ww-graph-test
  (let [g   (comp (partial just-graph ww-graph) vector)
        t1  (op "ax1")
        t2  (op "ax2")
        t3  (op "rx12")]
    (testing "write-write"
      ; Notice that T3 doesn't depend on T2, because we don't detect wr edges!
      (is (= {t1 [t2] t2 []}
             (g t1 t2 t3))))))

(deftest wr-graph-test
  (let [g (comp (partial just-graph wr-graph) vector)]
    (testing "basic read"
      (let [t1 (op "ax1")
            t2 (op "rx1")
            t3 (op "ax2")
            t4 (op "rx12")]
        ; Note that t2 doesn't precede t3, because the wr graph doesn't encode
        ; anti-dependency edges, and t1 doesn't precede t3, because there are
        ; no ww edges here.
        (is (= {t1 [t2], t2 [], t3 [t4], t4 []}
               (g t1 t2 t3 t4)))))))

(deftest graph-test
  (let [g (comp (partial just-graph graph) vector)
        ax1       {:type :ok, :value [[:append :x 1]]}
        ax2       {:type :ok, :value [[:append :x 2]]}
        ax1ay1    {:type :ok, :value [[:append :x 1] [:append :y 1]]}
        ax1ry1    {:type :ok, :value [[:append :x 1] [:r :y [1]]]}
        ax2ay1    {:type :ok, :value [[:append :x 2] [:append :y 1]]}
        ax2ay2    {:type :ok, :value [[:append :x 2] [:append :y 2]]}
        az1ax1ay1 {:type :ok, :value [[:append :z 1]
                                      [:append :x 1]
                                      [:append :y 1]]}
        rxay1     {:type :ok, :value [[:r :x nil] [:append :y 1]]}
        ryax1     {:type :ok, :value [[:r :y nil] [:append :x 1]]}
        rx121     {:type :ok, :value [[:r :x [1 2 1]]]}
        rx1ry1    {:type :ok, :value [[:r :x [1]] [:r :y [1]]]}
        rx1ay2    {:type :ok, :value [[:r :x [1]] [:append :y 2]]}
        ry12az3   {:type :ok, :value [[:r :y [1 2]] [:append :z 3]]}
        rz13      {:type :ok, :value [[:r :z [1 3]]]}
        rx        {:type :ok, :value [[:r :x nil]]}
        rx1       {:type :ok, :value [[:r :x [1]]]}
        rx12      {:type :ok, :value [[:r :x [1 2]]]}
        rx12ry1   {:type :ok, :value [[:r :x [1 2]] [:r :y [1]]]}
        rx12ry21  {:type :ok, :value [[:r :x [1 2]] [:r :y [2 1]]]}
        ]
    (testing "empty history"
      (is (= {} (g))))

    (testing "one append"
      (is (= {} (g ax1))))

    (testing "empty read"
      (is (= {} (g rx))))

    (testing "one append one read"
      (is (= {ax1 [rx1], rx1 []}
             (g ax1 rx1))))

    (testing "read empty, append, read"
      ; This verifies anti-dependencies.
      ; We need the third read in order to establish ax1's ordering
      (is (= {rx [ax1] ax1 [rx1] rx1 []}
             (g rx ax1 rx1))))

    (testing "append, append, read"
      ; This verifies write dependencies
      (is (= {ax1 [ax2], ax2 [rx12], rx12 []}
             (g ax2 ax1 rx12))))

    (testing "serializable figure 3 from Adya, Liskov, O'Neil"
      (is (= {az1ax1ay1 [rx1ay2 ry12az3]
              rx1ay2    [ry12az3]
              ry12az3   [rz13]
              rz13      []}
             (g az1ax1ay1 rx1ay2 ry12az3 rz13))))

    (testing "G0: write cycle"
      (let [t1 ax1ay1
            t2 ax2ay2
            ; Establishes that the updates from t1 and t2 were applied in
            ; different orders
            t3 rx12ry21]
        (is (= {t1 [t2 t3], t2 [t1 t3], t3 []}
               (g t1 t2 t3)))))

    ; TODO: we should do internal consistency checks here as well--see G1a and
    ; G1b.


    (testing "G1c: circular information flow"
      ; G0 is a special case of G1c, so for G1c we'll construct a cycle with a
      ; ww dependency on x and a wr dependency on y. The second transaction
      ; overwrites the first on x, but the second's write of y is visible to
      ; the first's read.
      (let [t1 ax1ry1
            t2 ax2ay1
            t3 rx12]
        (is (= {t1 [t2], t2 [t3 t1], t3 []}
               (g t1 t2 t3)))))

    (testing "G2: anti-dependency cycle"
      ; Here, two transactions observe the empty state of a key that the other
      ; transaction will append to.
      (is (= {rxay1 [ryax1 rx1ry1], ryax1 [rxay1 rx1ry1], rx1ry1 []}
             (g rxay1 ryax1 rx1ry1)))
      (is (= {:valid? false
              :scc-count 1
              :cycles ["Let:\n  T1 = {:type :ok, :value [[:r :x nil] [:append :y 1]]}\n  T2 = {:type :ok, :value [[:r :y nil] [:append :x 1]]}\n\nThen:\n  - T1 < T2, because T1 observed the initial (nil) state of :x, which T2 created by appending 1.\n  - However, T2 < T1, because T2 observed the initial (nil) state of :y, which T1 created by appending 1: a contradiction!"]}
             (checker/check (cycle/checker graph)
                            nil [rxay1 ryax1 rx1ry1] nil))))

    ; We can't infer anything about an info's nil reads: an :ok read of nil
    ; means we saw the initial state, but an :info read of nil means we don't
    ; know what was observed.
    (testing "info reads"
      ; T1 appends 2 to x after T2, so we can infer T2 < T1.
      ; However, T1's crashed read of y = nil does NOT mean T1 < T2.
      (let [t1 {:type :info, :value [[:append :x 2] [:r :y nil]]}
            t2 {:type :ok,   :value [[:append :x 1] [:append :y 1]]}
            t3 {:type :ok,   :value [[:r :x [1 2]] [:r :y [1]]]}]
        (is (= {t1 [t3], t2 [t3 t1], t3 []}
               (g t1 t2 t3)))))

    ; Special case: when there's only one append for a key, we can trivially
    ; infer the version order for that key, even if we never observe it in a
    ; read: it has to go from nil -> [x].
    (testing "single appends without reads"
      (is (= {rx [ax1] ax1 []} (g rx ax1)))
      ; But with two appends, we can't infer any more
      (is (= {} (g rx ax1 ax2))))

    (testing "duplicate inserts attempts"
      (let [ax1ry  {:index 0, :type :invoke, :value [[:append :x 1] [:r :y nil]]}
            ay2ax1 {:index 1, :type :invoke, :value [[:append :y 2] [:append :x 1]]}
            e (try+ (g ax1ry ay2ax1)
                    false
                    (catch [:type :duplicate-appends] e e))]
        (is (= ay2ax1 (:op e)))
        (is (= :x (:key e)))
        (is (= 1 (:value e)))))))

(deftest g1a-cases-test
  (testing "empty"
    (is (= [] (g1a-cases []))))
  (testing "valid and invalid reads"
    (let [t1 {:type :fail, :value [[:append :x 1]]}
          t2 (op "rx1ax2")
          t3 (op "rx12ry3")]
      (is (= [{:op        t2
               :mop       [:r :x [1]]
               :writer    t1
               :element   1}
              {:op        t3
               :mop       [:r :x [1 2]]
               :writer    t1
               :element   1}]
             (g1a-cases [t2 t3 t1]))))))

(deftest g1b-cases-test
  (testing "empty"
    (is (= [] (g1b-cases []))))

  (testing "valid and invalid reads"
    ; t1 has an intermediate append of 1 which should never be visible alone.
    (let [t1 (op "ax1ax2")
          t2 (op "rx1")
          t3 (op "rx12ry3")
          t4 (op "rx123")]
      (is (= [{:op        t2
               :mop       [:r :x [1]]
               :writer    t1
               :element   1}]
             (g1b-cases [t2 t3 t1 t4])))))

  (testing "internal reads"
    (let [t1 (op "ax1rx1ax2")]
      (is (= [] (g1b-cases [t1]))))))

(deftest internal-cases-test
  (testing "empty"
    (is (= nil (internal-cases []))))

  (testing "good"
    (is (= nil (internal-cases [{:type :ok, :value [[:r :y [5 6]]
                                                   [:append :x 3]
                                                   [:r :x [1 2 3]]
                                                   [:append :x 4]
                                                   [:r :x [1 2 3 4]]]}]))))

  (testing "read-append-read"
    (let [stale      {:type :ok, :value [[:r :x [1 2]]
                                         [:append :x 3]
                                         [:r :x [1 2]]]}
          bad-prefix {:type :ok, :value [[:r :x [1 2]]
                                         [:append :x 3]
                                         [:r :x [0 2 3]]]}
          extension  {:type :ok, :value [[:r :x [1 2]]
                                         [:append :x 3]
                                         [:r :x [1 2 3 4]]]}
          short-read {:type :ok, :value [[:r :x [1 2]]
                                         [:append :x 3]
                                         [:r :x [1]]]}]
    (is (= [{:op stale
             :mop [:r :x [1 2]]
             :expected [1 2 3]}
            {:op bad-prefix
             :mop [:r :x [0 2 3]]
             :expected [1 2 3]}
            {:op extension
             :mop [:r :x [1 2 3 4]]
             :expected [1 2 3]}
            {:op short-read
             :mop [:r :x [1]]
             :expected [1 2 3]}]
           (internal-cases [stale bad-prefix extension short-read])))))

  (testing "append-read"
    (let [disagreement {:type :ok, :value [[:append :x 3]
                                         [:r :x [1 2 3 4]]]}
          short-read {:type :ok, :value [[:append :x 3]
                                         [:r :x []]]}]
    (is (= [{:op disagreement
             :mop [:r :x [1 2 3 4]]
             :expected ['... 3]}
            {:op short-read
             :mop [:r :x []]
             :expected ['... 3]}]
           (internal-cases [disagreement short-read])))))

  (testing "FaunaDB example"
    (let [h [{:type :invoke, :f :txn, :value [[:append 0 6] [:r 0 nil]]
              :process 1, :index 20}
             {:type :ok, :f :txn, :value [[:append 0 6] [:r 0 nil]]
              :process 1, :index 21}]]
      (is (= [{:expected '[... 6],
               :mop [:r 0 nil],
               :op {:f :txn,
                    :index 21,
                    :process 1,
                    :type :ok,
                    :value [[:append 0 6] [:r 0 nil]]}}]
              (internal-cases h))))))

(deftest checker-test
  (let [c (fn [checker-opts history]
            (checker/check (checker checker-opts) nil history nil))]
    (testing "G0"
      (let [; A pure write cycle: x => t1, t2; but y => t2, t1
            t1 (op "ax1ay1")
            t2 (op "ax2ay2")
            t3 (op "rx12ry21")
            h [t1 t2 t3]
            msg "Let:\n  T1 = {:type :ok, :value [[:append :x 2] [:append :y 2]]}\n  T2 = {:type :ok, :value [[:append :x 1] [:append :y 1]]}\n\nThen:\n  - T1 < T2, because T2 appended 1 after T1 appended 2 to :y.\n  - However, T2 < T1, because T1 appended 2 after T2 appended 1 to :x: a contradiction!"]
        ; All checkers catch this.
        (is (= {:valid? false
                :anomaly-types [:G0]
                :anomalies {:G0 [msg]}}
               (c {:anomalies [:G0]} h)))
        (is (= {:valid? false
                :anomaly-types [:G0]
                :anomalies {:G0 [msg]}}
               (c {:anomalies [:G1]} h)))
        ; G2 doesn't actually include G0, but catches it anyway.
        (is (= {:valid? false
                :anomaly-types [:G0]
                :anomalies {:G0 [msg]}}
               (c {:anomalies [:G2]} h)))))

    (testing "G1a"
      (let [; T2 sees T1's failed write
            t1 {:type :fail, :value [[:append :x 1]]}
            t2 (op "rx1")
            h  [t1 t2]]
        ; G0 checker won't catch this
        (is (= {:valid? true} (c {:anomalies [:G0]} h)))
        ; G1 will
        (is (= {:valid? false
                :anomaly-types [:G1a]
                :anomalies {:G1a [{:op      t2
                                   :writer  t1
                                   :mop     [:r :x [1]]
                                   :element 1}]}}
               (c {:anomalies [:G1]} h)))
        ; G2 won't: even though the graph covers G1c, we don't do the specific
        ; G1a/b checks unless asked.
        (is (= {:valid? true}
               (c {:anomalies [:G2]} h)))))

    (testing "G1b"
      (let [; T2 sees T1's intermediate write
            t1 (op "ax1ax2")
            t2 (op "rx1")
            h  [t1 t2]]
        ; G0 checker won't catch this
        (is (= {:valid? true} (c {:anomalies [:G0]} h)))
        ; G1 will
        (is (= {:valid? false
                :anomaly-types [:G1b]
                :anomalies {:G1b [{:op      t2
                                   :writer  t1
                                   :mop     [:r :x [1]]
                                   :element 1}]}}
               (c {:anomalies [:G1]} h)))
        ; G2 won't: even though the graph covers G1c, we don't do the specific
        ; G1a/b checks unless asked.
        (is (= {:valid? true}
               (c {:anomalies [:G2]} h)))))

    (testing "G1c"
      (let [; T2 writes x after T1, but T1 observes T2's write on y.
            t1 (op "ax1ry1")
            t2 (op "ax2ay1")
            t3 (op "rx12ry1")
            h  [t1 t2 t3]
            msg "Let:\n  T1 = {:type :ok, :value [[:append :x 2] [:append :y 1]]}\n  T2 = {:type :ok, :value [[:append :x 1] [:r :y [1]]]}\n\nThen:\n  - T1 < T2, because T2 observed T1's append of 1 to key :y.\n  - However, T2 < T1, because T1 appended 2 after T2 appended 1 to :x: a contradiction!"]
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

    (testing "G-single"
      (let [t1  (op "ax1ay1")  ; T1 writes y after T1's read
            t2  (op "ax2ry")   ; T2 writes x after T1
            t3  (op "rx12")
            h   [t1 t2 t3]
            msg "Let:\n  T1 = {:type :ok, :value [[:append :x 2] [:r :y]]}\n  T2 = {:type :ok, :value [[:append :x 1] [:append :y 1]]}\n\nThen:\n  - T1 < T2, because T1 observed the initial (nil) state of :y, which T2 created by appending 1.\n  - However, T2 < T1, because T1 appended 2 after T2 appended 1 to :x: a contradiction!"]
        ; G0 and G1 won't catch this
        (is (= {:valid? true} (c {:anomalies [:G0]} h)))
        (is (= {:valid? true} (c {:anomalies [:G1]} h)))
        ; But G-single and G2 will!
        (is (= {:valid? false
                :anomaly-types [:G-single]
                :anomalies {:G-single [msg]}}
               (c {:anomalies [:G-single]} h)))
        (is (= {:valid? false
                :anomaly-types [:G-single]
                :anomalies {:G-single [msg]}}
               (c {:anomalies [:G2]} h)))))

    (testing "G2"
      (let [; A pure anti-dependency cycle
            t1 (op "ax1ry")
            t2 (op "ay1rx")
            h  [t1 t2]]
        ; G0 and G1 won't catch this
        (is (= {:valid? true} (c {:anomalies [:G0]} h)))
        (is (= {:valid? true} (c {:anomalies [:G1]} h)))
        ; But G2 will
        (is (= {:valid? false
                :anomaly-types [:G2]
                :anomalies {:G2 ["Let:\n  T1 = {:type :ok, :value [[:append :x 1] [:r :y]]}\n  T2 = {:type :ok, :value [[:append :y 1] [:r :x]]}\n\nThen:\n  - T1 < T2, because T1 observed the initial (nil) state of :y, which T2 created by appending 1.\n  - However, T2 < T1, because T2 observed the initial (nil) state of :x, which T1 created by appending 1: a contradiction!"]}}
               (c {:anomalies [:G2]} h)))))

    (testing "Strict-1SR violation"
      (let [; T1 anti-depends on T2, but T1 happens first in wall-clock order.
            t1  {:index 0, :type :invoke, :value [[:append :x 2]]}
            t1' {:index 1, :type :ok,     :value [[:append :x 2]]}
            t2  {:index 2, :type :invoke, :value [[:r :x nil]]}
            t2' {:index 3, :type :ok,     :value [[:r :x [1]]]}
            t3  {:index 4, :type :invoke, :value [[:r :x nil]]}
            t3' {:index 5, :type :ok,     :value [[:r :x [1 2]]]}
            h [t1 t1' t2 t2' t3 t3']]
        ; G2 won't catch this by itself
        (is (= {:valid? true}
               (c {:anomalies [:G2]} h)))
        ; But it will if we introduce a realtime graph component
        (is (= {:valid? false
                :anomaly-types [:G-single]
                :anomalies {:G-single ["Let:\n  T1 = {:index 3, :type :ok, :value [[:r :x [1]]]}\n  T2 = {:index 1, :type :ok, :value [[:append :x 2]]}\n\nThen:\n  - T1 < T2, because T1 did not observe T2's append of 2 to :x.\n  - However, T2 < T1, because T2 completed at index 1, before the invocation of T1, at index 2: a contradiction!"]}}
               (c {:anomalies [:G2]
                   :additional-graphs [cycle/realtime-graph]}
                  h)))))

    (testing "contradictory read orders"
      (let [t1 (op "ax1ry1")  ; read t3's ay1
            t2 (op "ax2")
            t3 (op "ax3ay1")  ; append of x happens later
            t4 (op "rx13")
            t5 (op "rx123")
            h [t1 t2 t3 t4 t5]]
        (is (= {:valid? false
                :anomaly-types [:G1c :incompatible-order]
                :anomalies
                {:incompatible-order [{:key :x, :values [[1 3] [1 2 3]]}]
                 :G1c ["Let:\n  T1 = {:type :ok, :value [[:append :x 3] [:append :y 1]]}\n  T2 = {:type :ok, :value [[:append :x 1] [:r :y [1]]]}\n\nThen:\n  - T1 < T2, because T2 observed T1's append of 1 to key :y.\n  - However, T2 < T1, because T1 appended 3 after T2 appended 1 to :x: a contradiction!"]}}
                (c {:anomalies [:G1]} h)))))

    (testing "duplicated elements"
      ; This is an instance of G1c
      (let [t1 (op "ax1ry1") ; read t2's write of y
            t2 (op "ax2ay1")
            t3 (op "rx121")
            h  [t1 t2 t3]]
        (is (= {:valid? false
                :anomaly-types [:G1c :duplicate-elements]
                :anomalies {:duplicate-elements [{:op t3
                                                  :mop [:r :x [1 2 1]]
                                                  :duplicates {1 2}}]
                            :G1c ["Let:\n  T1 = {:type :ok, :value [[:append :x 2] [:append :y 1]]}\n  T2 = {:type :ok, :value [[:append :x 1] [:r :y [1]]]}\n\nThen:\n  - T1 < T2, because T2 observed T1's append of 1 to key :y.\n  - However, T2 < T1, because T1 appended 2 after T2 appended 1 to :x: a contradiction!"]}}
               (c {:anomalies [:G1]} h)))))

    (testing "internal consistency violation"
      (let [t1 (op "ax3rx1234")
            h  [t1]]
        (is (= {:valid? false
                :anomaly-types [:internal]
                :anomalies {:internal [{:op t1
                                        :mop [:r :x [1 2 3 4]]
                                        :expected '[... 3]}]}}
               (c {:anomalies [:G1]} h)))))))

(deftest merge-order-test
  (is (= [] (merge-orders [] [])))
  (is (= [1 2 3] (merge-orders [1 2 3] [])))
  (is (= [2 3 4] (merge-orders [] [2 3 4])))
  (is (= [1 2 3] (merge-orders [1 2 3] [1 2 3])))
  (is (= [1 4 9] (merge-orders [1 4] [1 4 9])))
  (is (= [1 4 5] (merge-orders [1 4 5] [1])))
  (is (= [1 5 6] (merge-orders [1 2 5 6] [1 3 5 6])))
  (is (= [1 3]   (merge-orders [1 2] [1 3])))
  (testing "dups"
    (is (= [1 2 3] (merge-orders [1 2 2 3] [])))
    (is (= [1 2 3 5] (merge-orders [1 2 3 2]
                                   [1 2 3 2 5])))))
