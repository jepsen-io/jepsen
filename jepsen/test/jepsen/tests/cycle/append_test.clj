(ns jepsen.tests.cycle.append-test
  (:require [jepsen.tests.cycle :refer :all]
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
       ->clj
       (map (fn [[k vs]]
              [(dissoc k :index)
               (map #(dissoc % :index) vs)]))
       (into {})))

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
             (checker/check (checker graph)
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
    (prn) (prn)
    (testing "single appends without reads"
      (is (= {rx [ax1] ax1 []} (g rx ax1)))
      ; But with two appends, we can't infer any more
      (is (= {} (g rx ax1 ax2))))

    (testing "reads with duplicates"
      (let [e (try+ (g rx121)
                    false
                    (catch [:type :duplicate-elements] e e))]
        (is (= (assoc rx121 :index 0) (:op e)))
        (is (= :x (:key e)))
        (is (= [1 2 1] (:value e)))
        (is (= {1 2} (:duplicates e)))))

    (testing "duplicate inserts attempts"
      (let [ax1ry  {:index 0, :type :invoke, :value [[:append :x 1] [:r :y nil]]}
            ay2ax1 {:index 1, :type :invoke, :value [[:append :y 2] [:append :x 1]]}
            e (try+ (g ax1ry ay2ax1)
                    false
                    (catch [:type :duplicate-appends] e e))]
        (is (= ay2ax1 (:op e)))
        (is (= :x (:key e)))
        (is (= 1 (:value e)))))

    (testing "incompatible orders"
      (let [rx12  {:index 0, :type :ok, :value [[:r :x [1 2]]]}
            rx134 {:index 1, :type :ok, :value [[:r :x [1 3 4]]]}
            e (try+ (g rx12 rx134)
                    false
                    (catch [:type :no-total-state-order] e e))]
        (is (= [{:key :x
                 :values [[1 2] [1 3 4]]}]
               (:errors e)))
        (is (= {:valid?   false
                :type     :no-total-state-order
                :message  "observed mutually incompatible orders of appends"
                :errors   [{:key :x, :values [[1 2] [1 3 4]]}]}
               (checker/check (checker graph)
                              nil [rx12 rx134] nil)))))))
