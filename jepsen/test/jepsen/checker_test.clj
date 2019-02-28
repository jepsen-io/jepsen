(ns jepsen.checker-test
  (:refer-clojure :exclude [set])
  (:use jepsen.checker
        clojure.test)
  (:require [knossos [history :as history]
             [model :as model]
             [core :refer [ok-op invoke-op fail-op]]
             [op :as op]]
            [multiset.core :as multiset]
            [jepsen.checker.perf :as cp]
            [jepsen.util :as util]))

(deftest queue-test
  (testing "empty"
    (is (:valid? (check (queue nil) nil [] {}))))

  (testing "Possible enqueue but no dequeue"
    (is (:valid? (check (queue (model/unordered-queue)) nil
                        [(invoke-op 1 :enqueue 1)] {}))))

  (testing "Definite enqueue but no dequeue"
    (is (:valid? (check (queue (model/unordered-queue)) nil
                        [(ok-op 1 :enqueue 1)] {}))))

  (testing "concurrent enqueue/dequeue"
    (is (:valid? (check (queue (model/unordered-queue)) nil
                        [(invoke-op 2 :dequeue nil)
                         (invoke-op 1 :enqueue 1)
                         (ok-op     2 :dequeue 1)] {}))))

  (testing "dequeue but no enqueue"
    (is (not (:valid? (check (queue (model/unordered-queue)) nil
                             [(ok-op 1 :dequeue 1)] {}))))))

(deftest total-queue-test
  (testing "empty"
    (is (:valid? (check (total-queue) nil [] {}))))

  (testing "sane"
    (is (= (check (total-queue) nil
                  [(invoke-op 1 :enqueue 1)
                   (invoke-op 2 :enqueue 2)
                   (ok-op     2 :enqueue 2)
                   (invoke-op 3 :dequeue 1)
                   (ok-op     3 :dequeue 1)
                   (invoke-op 3 :dequeue 2)
                   (ok-op     3 :dequeue 2)]
                  {})
           {:valid?           true
            :duplicated       (multiset/multiset)
            :lost             (multiset/multiset)
            :unexpected       (multiset/multiset)
            :recovered        (multiset/multiset 1)
            :attempt-count       2
            :acknowledged-count  1
            :ok-count            2
            :unexpected-count    0
            :lost-count          0
            :duplicated-count    0
            :recovered-count     1})))

  (testing "pathological"
    (is (= (check (total-queue) nil
                  [(invoke-op 1 :enqueue :hung)
                   (invoke-op 2 :enqueue :enqueued)
                   (ok-op     2 :enqueue :enqueued)
                   (invoke-op 3 :enqueue :dup)
                   (ok-op     3 :enqueue :dup)
                   (invoke-op 4 :dequeue nil) ; nope
                   (invoke-op 5 :dequeue nil)
                   (ok-op     5 :dequeue :wtf)
                   (invoke-op 6 :dequeue nil)
                   (ok-op     6 :dequeue :dup)
                   (invoke-op 7 :dequeue nil)
                   (ok-op     7 :dequeue :dup)]
                  {})
           {:valid?           false
            :lost             (multiset/multiset :enqueued)
            :unexpected       (multiset/multiset :wtf)
            :recovered        (multiset/multiset)
            :duplicated       (multiset/multiset :dup)
            :acknowledged-count 2
            :attempt-count    3
            :ok-count         1
            :lost-count       1
            :unexpected-count 1
            :duplicated-count 1
            :recovered-count  0}))))

(deftest counter-test
  (testing "empty"
    (is (= (check (counter) nil [] {})
           {:valid? true
            :reads  []
            :errors []})))

  (testing "initial read"
    (is (= (check (counter)
                  nil
                  [(invoke-op 0 :read nil)
                   (ok-op     0 :read 0)]
                  {})
           {:valid? true
            :reads  [[0 0 0]]
            :errors []})))

  (testing "ignore failed ops"
    (is (= (check (counter) nil
                  [(invoke-op 0 :add 1)
                   (fail-op   0 :add 1)
                   (invoke-op 0 :read nil)
                   (ok-op     0 :read 0)]
                  {})
           {:valid? true
            :reads  [[0 0 0]]
            :errors []})))

  (testing "initial invalid read"
    (is (= (check (counter) nil
                  [(invoke-op 0 :read nil)
                   (ok-op     0 :read 1)]
                  {})
           {:valid? false
            :reads  [[0 1 0]]
            :errors [[0 1 0]]})))

  (testing "interleaved concurrent reads and writes"
    (is (= (check (counter) nil
                  [(invoke-op 0 :read nil)
                   (invoke-op 1 :add 1)
                   (invoke-op 2 :read nil)
                   (invoke-op 3 :add 2)
                   (invoke-op 4 :read nil)
                   (invoke-op 5 :add 4)
                   (invoke-op 6 :read nil)
                   (invoke-op 7 :add 8)
                   (invoke-op 8 :read nil)
                   (ok-op     0 :read 6)
                   (ok-op     1 :add 1)
                   (ok-op     2 :read 0)
                   (ok-op     3 :add 2)
                   (ok-op     4 :read 3)
                   (ok-op     5 :add 4)
                   (ok-op     6 :read 100)
                   (ok-op     7 :add 8)
                   (ok-op     8 :read 15)]
                  {})
           {:valid? false
            :reads  [[0 6 15] [0 0 15] [0 3 15] [0 100 15] [0 15 15]]
            :errors [[0 100 15]]})))

  (testing "rolling reads and writes"
    (is (= (check (counter) nil
                  [(invoke-op 0 :read nil)
                   (invoke-op 1 :add  1)
                   (ok-op     0 :read 0)
                   (invoke-op 0 :read nil)
                   (ok-op     1 :add  1)
                   (invoke-op 1 :add  2)
                   (ok-op     0 :read 3)
                   (invoke-op 0 :read nil)
                   (ok-op     1 :add  2)
                   (ok-op     0 :read 5)] {})
           {:valid? false
            :reads  [[0 0 1] [0 3 3] [1 5 3]]
            :errors [[1 5 3]]}))))

(deftest compose-test
  (is (= (check (compose {:a (unbridled-optimism) :b (unbridled-optimism)})
                nil nil {})
         {:a {:valid? true}
          :b {:valid? true}
          :valid? true})))

(deftest bucket-points-test
  (is (= (cp/bucket-points 2
                           [[1 :a]
                            [7 :g]
                            [5 :e]
                            [2 :b]
                            [3 :c]
                            [4 :d]
                            [6 :f]])
         {1 [[1 :a]]
          3 [[2 :b]
             [3 :c]]
          5 [[5 :e]
             [4 :d]]
          7 [[7 :g]
             [6 :f]]})))

(deftest latencies->quantiles-test
  (is (= {0 [[5/2 0]  [15/2 20] [25/2 25]]
          1 [[5/2 10] [15/2 25] [25/2 25]]}
         (cp/latencies->quantiles 5 [0 1] (partition 2 [0 0
                                                        1 10
                                                        2 1
                                                        3 1
                                                        4 1
                                                        5 20
                                                        6 21
                                                        7 22
                                                        8 25
                                                        9 25
                                                        10 25])))))

(defn perf-gen
  ([latency]
   (perf-gen latency nil))
  ([latency nemesis?]
   (let [f (rand-nth [:write :read])
         proc (rand-int 100)
         time (* 1e9 (rand-int 100))
         type (rand-nth [:ok :ok :ok :ok :ok
                         :fail :info :info])]
     [{:process proc, :type :invoke, :f f, :time time}
      {:process proc, :type type,    :f f, :time
       (+ time latency)}])))

(deftest perf-test
  (let [history (->> (repeatedly #(/ 1e9 (inc (rand-int 1000))))
                     (mapcat perf-gen)
                     (take 10000)
                     vec)]

    (testing "can render latency-graph"
      (is (= (check (latency-graph)
                    {:name "latency graph"
                     :start-time 0}
                    history
                    {})
             {:valid? true})))

    (testing "can render rate-graph"
      (is (= (check (rate-graph)
                    {:name "rate graph"
                     :start-time 0}
                    history
                    {})
             {:valid? true})))

    (testing "can render combined perf graph"
      (is (= (check (perf)
                    {:name "perf graph"
                     :start-time 0}
                    history
                    {})
             {:latency-graph {:valid? true},
              :rate-graph {:valid? true},
              :valid? true})))

    (testing "can render a :start :stop nemesis region without opts"
      (let [checker (perf)
            test    {:name "nemesis compatibility perf test"
                     :start-time 0}
            nemesis-ops [{:type :info
                          :process :nemesis
                          :f :start
                          :value nil
                          :time (* 1e9 5)}
                         {:type :info
                          :process :nemesis
                          :f :start
                          :value [:isolated {"n2" #{"n1" "n4" "n3"}, "n5" #{"n1" "n4" "n3"}, "n1" #{"n2" "n5"}, "n4" #{"n2" "n5"}, "n3" #{"n2" "n5"}}]
                          :time (* 1e9 20)}
                         {:type :info
                          :process :nemesis
                          :f :stop
                          :value nil
                          :time (* 1e9 50)}
                         {:type :info
                          :process :nemesis
                          :f :stop
                          :value :network-healed
                          :time (* 1e9 90)}]
            history (apply conj history nemesis-ops)]
        (is (= (check checker test history {})
               {:latency-graph {:valid? true},
                :rate-graph {:valid? true},
                :valid? true}))))

    (testing "can render single nemesis events as bars"
      (let [checker (perf {:nemeses #{{:name "solo nemeses"}}})
            test    {:name "nemeses solo event"
                     :start-time 0}
            nemesis-ops [{:type :info
                          :process :nemesis
                          :f :nemesize
                          :value :spooky!
                          :time (* 1e9 20)}
                         {:type :info
                          :process :nemesis
                          :f :nemesize
                          :value :woah!
                          :time (* 1e9 80)}]
            history (apply conj history nemesis-ops)]
        (is (= (check checker test history {})
               {:latency-graph {:valid? true},
                :rate-graph {:valid? true},
                :valid? true}))))

    (testing "can render nemeses with custom styling"
      (let [checker (perf {:nemeses #{{:name "cool nemesis 8)"
                                       :fill-color "#6DB6FE"
                                       :transparency 0.5
                                       :line-color "#6DB6FE"
                                       :line-width 2}}})
            test    {:name "nemeses styling perf test"
                     :start-time 0}
            nemesis-ops [{:type :info
                          :process :nemesis
                          :f :start
                          :value nil
                          :time (* 1e9 5)}
                         {:type :info
                          :process :nemesis
                          :f :start
                          :value [:isolated {"n2" #{"n1" "n4" "n3"}, "n5" #{"n1" "n4" "n3"}, "n1" #{"n2" "n5"}, "n4" #{"n2" "n5"}, "n3" #{"n2" "n5"}}]
                          :time (* 1e9 20)}
                         {:type :info
                          :process :nemesis
                          :f :stop
                          :value nil
                          :time (* 1e9 50)}
                         {:type :info
                          :process :nemesis
                          :f :stop
                          :value :network-healed
                          :time (* 1e9 90)}]
            history (apply conj history nemesis-ops)]
        (is (= (check checker test history {})
               {:latency-graph {:valid? true},
                :rate-graph {:valid? true},
                :valid? true}))))

    (testing "can render multiple nemesis regions"
      (let [checker (perf {:nemeses #{{:name "1"
                                       :start #{:start1}
                                       :stop  #{:stop1}
                                       :fill-color "#800080"
                                       :transparency 0.2}
                                      {:name "2"
                                       :start #{:start2.1 :start2.2}
                                       :stop  #{:stop2.1 :stop2.2}
                                       :fill-color "#87A96B"
                                       :transparency 0.2}}})
            test    {:name "nemeses multiregions perf test"
                     :start-time 0}

            ;; Hnnnnnnnnnng we should simplify this... ugly brute force
            nemesis-ops [{:type :info
                          :process :nemesis
                          :f :start1
                          :value nil
                          :time (* 1e9 5)}
                         {:type :info
                          :process :nemesis
                          :f :start1
                          :value [:isolated {"n2" #{"n1" "n4" "n3"}, "n5" #{"n1" "n4" "n3"}, "n1" #{"n2" "n5"}, "n4" #{"n2" "n5"}, "n3" #{"n2" "n5"}}]
                          :time (* 1e9 20)}
                         {:type :info
                          :process :nemesis
                          :f :stop1
                          :value nil
                          :time (* 1e9 40)}
                         {:type :info
                          :process :nemesis
                          :f :stop1
                          :value :network-healed
                          :time (* 1e9 60)}

                         {:type :info
                          :process :nemesis
                          :f :start2.1
                          :value nil
                          :time (* 1e9 30)}
                         {:type :info
                          :process :nemesis
                          :f :start2.2
                          :value [:isolated {"n2" #{"n1" "n4" "n3"}, "n5" #{"n1" "n4" "n3"}, "n1" #{"n2" "n5"}, "n4" #{"n2" "n5"}, "n3" #{"n2" "n5"}}]
                          :time (* 1e9 65)}
                         {:type :info
                          :process :nemesis
                          :f :stop2.2
                          :value nil
                          :time (* 1e9 45)}
                         {:type :info
                          :process :nemesis
                          :f :stop2.1
                          :value :network-healed
                          :time (* 1e9 95)}]
            history (apply conj history nemesis-ops)]
        (is (= (check checker test history {})
               {:latency-graph {:valid? true},
                :rate-graph {:valid? true},
                :valid? true}))))

    (testing "fill-color bug"
      (let [checker (perf {:nemeses #{{:name "fill-color bug"
                                       :fill-color "#6DB6FE"
                                       :start #{:start-bug}
                                       :stop #{:stop-bug}}
                                      {:name "control"
                                       :start #{:start-control}
                                       :stop #{:stop-control}}}})
            test    {:name "nemesis fill-color bug"
                     :start-time 0}
            nemesis-ops [{:type :info
                          :process :nemesis
                          :f :start-control
                          :value nil
                          :time (* 1e9 5)}
                         {:type :info
                          :process :nemesis
                          :f :start-control
                          :value [:isolated {"n2" #{"n1" "n4" "n3"}, "n5" #{"n1" "n4" "n3"}, "n1" #{"n2" "n5"}, "n4" #{"n2" "n5"}, "n3" #{"n2" "n5"}}]
                          :time (* 1e9 20)}
                         {:type :info
                          :process :nemesis
                          :f :stop-control
                          :value nil
                          :time (* 1e9 50)}
                         {:type :info
                          :process :nemesis
                          :f :stop-control
                          :value :network-healed
                          :time (* 1e9 90)}]
            history (apply conj history nemesis-ops)]
        (is (= (check checker test history {})
               {:latency-graph {:valid? true},
                :rate-graph {:valid? true},
                :valid? true}))))))

(deftest clock-plot-test
  (check (clock-plot)
         {:name       "clock plot test"
          :start-time 0}
         [{:process :nemesis, :time 500000000,  :clock-offsets {"n1" 2.1}}
          {:process :nemesis, :time 1000000000, :clock-offsets {"n1" 0
                                                                "n2" -3.1}}
          {:process :nemesis, :time 1500000000, :clock-offsets {"n1" 1
                                                                "n2" -2}}
          {:process :nemesis, :time 2000000000, :clock-offsets {"n1" 2
                                                              "n2" -4.1}}]
         {}))

(defn history
  "Takes a sequence of operations and adds times and indexes."
  [h]
  (let [h (history/index h)]
    (condp = (count h)
      0 h
      1 [(assoc (first h) :time 0)]
      (reduce (fn [h op]
                (conj h (assoc op :time (+ (:time (peek h))
                                           1000000))))
              [(assoc (first h) :time 0)]
              (rest h)))))

(deftest set-full-test
  ; Helper fn to check a history
  (let [c (fn [h] (check (set-full) nil (history h) {}))]
    (testing "never read"
      (is (= {:lost             []
              :attempt-count    1
              :lost-count       0
              :never-read       [0]
              :never-read-count 1
              :stale-count      0
              :stale            []
              :worst-stale      []
              :stable-count     0
              :duplicated-count 0
              :duplicated       {}
              :valid?           :unknown}
             (c [(invoke-op 0 :add 0)
                 (ok-op 0 :add 0)]))))

    (let [a   (invoke-op 0 :add 0)
          a'  (ok-op 0 :add 0)
          r   (invoke-op 1 :read nil)
          r+  (ok-op 1 :read #{0})
          r-  (ok-op 1 :read #{})]
      (testing "never confirmed, never read"
        (is (= {:valid?           :unknown
                :attempt-count    1
                :lost             []
                :lost-count       0
                :never-read       [0]
                :never-read-count 1
                :stale-count      0
                :stale            []
                :duplicated-count 0
                :duplicated       {}
                :worst-stale      []
                :stable-count     0}
               (c [a r r-]))))
      (testing "successful read either concurrently or after"
        (is (= {:valid?           true
                :attempt-count    1
                :lost             []
                :lost-count       0
                :never-read       []
                :never-read-count 0
                :stale-count      0
                :stale            []
                :worst-stale      []
                :duplicated-count 0
                :duplicated       {}
                :stable-count     1
                :stable-latencies {0 0, 0.5 0, 0.95 0, 0.99 0, 1 0}}
               (c [r a r+ a']) ; Concurrent read before
               (c [r a a' r+]) ; Concurrent read outside
               (c [a r r+ a']) ; Concurrent read inside
               (c [a r a' r+]) ; Concurrent read after
               (c [a a' r r+]) ; Subsequent read
               )))

      (testing "Absent read after"
        (is (= {:valid?           false
                :attempt-count    1
                :lost             [0]
                :lost-count       1
                :never-read       []
                :never-read-count 0
                :stale-count      0
                :stale            []
                :worst-stale      []
                :stable-count     0
                :duplicated-count 0
                :duplicated       {}
                :lost-latencies  {0 0, 0.5 0, 0.95 0, 0.99 0, 1 0}}
               (c [a a' r r-]))))

    (testing "Absent read concurrently"
        (is (= {:valid?           :unknown
                :attempt-count    1
                :lost             []
                :lost-count       0
                :never-read       [0]
                :never-read-count 1
                :stale-count      0
                :stale            []
                :worst-stale      []
                :duplicated-count 0
                :duplicated       {}
                :stable-count     0}
               (c [r a r- a']) ; Read before
               (c [r a a' r-]) ; Read outside
               (c [a r r- a']) ; Read inside
               (c [a r a' r-]) ; Read after
               ))))

    (let [a0    (invoke-op  0 :add 0)
          a0'   (ok-op      0 :add 0)
          a1    (invoke-op  1 :add 1)
          a1'   (ok-op      1 :add 1)
          r2    (invoke-op  2 :read nil)
          r3    (invoke-op  3 :read nil)
          r2'   (ok-op      2 :read #{})
          r3'   (ok-op      3 :read #{})
          r2'0  (ok-op      2 :read #{0})
          r3'0  (ok-op      3 :read #{0})
          r2'1  (ok-op      2 :read #{1})
          r3'1  (ok-op      3 :read #{1})
          r2'01 (ok-op      2 :read #{0 1})
          r3'01 (ok-op      3 :read #{0 1})]
      (testing "write, present, missing"
        (is (= {:valid? false
                :attempt-count 2
                :lost [0 1]
                :lost-count 2
                :never-read []
                :never-read-count 0
                :stale-count 0
                :stale       []
                :worst-stale []
                :stable-count 0
                :duplicated-count 0
                :duplicated       {}
                :lost-latencies {0 3, 0.5 4, 0.95 4, 0.99 4,
                                 1 4}}
               ; We write a0 and a1 concurrently, reading 1 before a1
               ; completes. Then we read both, 0, then nothing.
               (c [a0 a1 r2 r2'1 a0' a1' r2 r2'01 r2 r2'0 r2 r2']))))
      (testing "write, flutter, stable/lost"
        (is (= {:valid? false
                :attempt-count 2
                :lost [0]
                :lost-count 1
                :never-read []
                :never-read-count 0
                :duplicated-count 0
                :duplicated       {}
                ; 1 should have been done at 4, is missing at time 6000, and
                ; recovered at 7000.
                :stale-count 1
                :stale       [1]
                :worst-stale [{:element         1
                               :known           (assoc r2'1
                                                       :index 4
                                                       :time 4000000)
                               :last-absent     (assoc r2
                                                       :index 6
                                                       :time 6000000)
                               :lost-latency    nil
                               :outcome         :stable
                               :stable-latency  2}]
                :stable-count 1
                ; We know 0 is done at time 1000, but it goes missing after
                ; 6000.
                :lost-latencies {0 5, 0.5 5, 0.95 5,
                                  0.99 5, 1 5}
                ; 1 is known at time 4 (not 5! The read sees it before the
                ; write completes). It is missing at 6000, and recovered at
                ; 7000.
                :stable-latencies {0 2, 0.5 2, 0.95 2,
                                   0.99 2, 1 2}}
               ; We write a0, then a1, reading 1 before a1 completes, then just
               ; 0 and 1 concurrently, but 1 starting later. This is a recovery
               ; of 1, but 0 should be lost, because there's no time after
               ; which an operation can begin and always observe 0.
               ;
               ; t 0  1   2  3  4    5   6  7  8    9
               (c [a0 a0' a1 r2 r2'1 a1' r2 r3 r3'1 r2'0])))))))
