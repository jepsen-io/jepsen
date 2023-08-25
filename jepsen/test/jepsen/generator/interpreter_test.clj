(ns jepsen.generator.interpreter-test
  (:refer-clojure :exclude [run!])
  (:require [clojure [data :refer [diff]]
                     [datafy :refer [datafy]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.generator :as gen]
            [jepsen.generator.interpreter :refer :all]
            [jepsen [common-test :refer [quiet-logging]]
                    [core :as jepsen]
                    [client :refer [Client]]
                    [history :as h]
                    [nemesis :refer [Nemesis]]
                    [util :as util]
                    [tests :as tests]]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [slingshot.slingshot :refer [try+ throw+]]))

(use-fixtures :once quiet-logging)

(def base-test
  (assoc tests/noop-test
         :concurrency 10))

(defn ok-client
  []
  (reify Client
    (open! [this test node] this)
    (setup! [this test])
    (invoke! [this test op]
      (Thread/sleep 10)
      (assoc op :type :ok))
    (teardown! [this test])
    (close! [this test])))

(defn info-nemesis
  []
  (reify Nemesis
    (setup! [this test] this)
    (invoke! [this test op] op)
    (teardown! [this test])))

(deftest ^:perf perf-test
  (let [time-limit      15
        concurrency     1024
        test (assoc base-test
                    :concurrency concurrency
                    :client (reify Client
                              (open! [this test node] this)
                              (setup! [this test])
                              (invoke! [this test op]
                                (assoc op :type
                                       (condp < (rand)
                                         ; 1 in 10 ops crash
                                         0.9 :info
                                         ; 3 in 10 fail
                                         0.6 :fail
                                         ; 6 in 10 succeed
                                         :ok)
                                       :value :foo))
                              (teardown! [this test])
                              (close! [this test]))
              :nemesis  (reify Nemesis
                          (setup! [this test] this)
                          (invoke! [this test op]
                            (assoc op :type :info, :value :broken))
                          (teardown! [this test]))
              :generator
              (gen/phases
                (->> (gen/reserve (long (/ concurrency 4))
                                  (->> (range)
                                         (map (fn [x] {:f :write, :value x})))

                                  (long (/ concurrency 2))
                                  (fn cas-gen [test ctx]
                                        {:f      :cas
                                         :value  [(rand-int 5) (rand-int 5)]})

                                  (repeat {:f :read}))
                     (gen/nemesis
                       (gen/mix [(gen/repeat {:type :info, :f :break})
                                 (gen/repeat {:type :info, :f :repair})]))
                     (gen/time-limit time-limit))
                (gen/log "Recovering")))
        h    (:history (jepsen/run! test))
        rate (float (/ (count h) 2 time-limit))]
    (prn :generator-interpeter-rate rate)
    (is (< 10000 rate))))

(deftest run!-test
  (let [time-limit     1
        sleep-duration 1/10
        test (assoc base-test
              :name "interpreter-run-test"
              :client (reify Client
                        (open! [this test node] this)
                        (setup! [this test])
                        (invoke! [this test op]
                          (assoc op :type (rand-nth [:ok :info :fail])
                                 :value :foo))
                        (teardown! [this test])
                        (close! [this test]))
              :nemesis  (reify Nemesis
                          (setup! [this test] this)
                          (invoke! [this test op]
                            (assoc op :type :info, :value :broken))
                          (teardown! [this test]))
              :generator
              (gen/phases
                (->> (gen/reserve 2 (->> (range)
                                         (map (fn [x] {:f :write, :value x})))
                                  5 (fn cas-gen [test ctx]
                                      {:f      :cas
                                       :value  [(rand-int 5) (rand-int 5)]})
                                  (repeat {:f :read}))
                     (gen/nemesis
                       (gen/mix [(gen/repeat {:type :info, :f :break})
                                 (gen/repeat {:type :info, :f :repair})]))
                     (gen/time-limit time-limit))
                (gen/log "Recovering")
                (gen/nemesis {:type :info, :f :recover})
                (gen/sleep sleep-duration)
                (gen/log "done recovering; final read")
                (gen/clients (gen/until-ok (repeat {:f :read})))))
        h    (:history (jepsen/run! test))
        nemesis-ops (filter (comp #{:nemesis} :process) h)
        client-ops  (remove (comp #{:nemesis} :process) h)]

    (testing "general structure"
      (is (sequential? h))
      (is (indexed? h))
      (is (counted? h))
      (is (= #{:invoke :ok :info :fail} (set (map :type h))))
      (is (every? integer? (map :time h))))

    (testing "timestamps"
      (is (distinct? (map :time h)))
      (is (= (sort (map :time h)) (map :time h))))

    (testing "client ops"
      (is (seq client-ops))
      (is (every? #{:write :read :cas} (map :f client-ops))))

    (testing "nemesis ops"
      (is (seq nemesis-ops))
      (is (every? #{:break :repair :recover} (map :f nemesis-ops))))

    (testing "mixed, recover, final read"
      (let [recoveries (keep-indexed (fn [index op]
                                       (when (= :recover (:f op))
                                         index))
                                     h)
            recovery (first recoveries)
            mixed    (take recovery h)
            mixed-clients (filter (comp number? :process) mixed)
            mixed-nemesis (remove (comp number? :process) mixed)
            final    (drop (+ 2 recovery) h)]

        (testing "mixed"
          (is (pos? (count mixed)))
          (is (some #{:nemesis} (map :process mixed)))
          (is (some number? (map :process mixed)))
          (is (= #{:invoke :ok :info :fail} (set (map :type mixed))))
          (is (= #{:write :read :cas} (set (map :f mixed-clients))))
          (is (= #{:break :repair} (set (map :f mixed-nemesis))))

          (let [by-f (group-by :f mixed-clients)
                n    (count mixed-clients)]
            ;(pprint (util/map-vals (comp float #(/ % n) count) by-f))
            (testing "writes"
              (is (< 1/10 (/ (count (by-f :write)) n) 3/10))
              (is (distinct? (map :value (filter (comp #{:invoke} :type)
                                                (by-f :write))))))
            (testing "cas"
              (is (< 4/10 (/ (count (by-f :cas)) n) 6/10))
              (is (every? vector? (map :value (filter (comp #{:invoke} :type)
                                                      (by-f :cas))))))

            (testing "read"
              (is (< 2/10 (/ (count (by-f :read)) n) 4/10)))))

        (testing "recovery"
          (is (= 2 (count recoveries)))
          (is (= (inc (first recoveries)) (second recoveries))))

        (testing "final read"
          (is (pos? (count final)))
          (is (every? number? (map :process final)))
          (is (every? (comp #{:read} :f) final))
          (is (pos? (count (filter (comp #{:ok} :type) final)))))))

    (testing "fast enough"
      ; On my box, ~18K ops/sec. This is a good place to profile, I
      ; think--there's some low-hanging fruit in OnThreads `update`, which
      ; calls process->thread with a linear cost.
      ; (prn (float (/ (count h) time-limit)))
      (is (< 5000 (float (/ (count h) time-limit)))))
    ))

(deftest run!-end-process-test
  ; When a client explicitly signifies that it'd like to end the process, we
  ; should spawn a new one.
  (let [test (assoc base-test
                    :name "interpreter-run-end-process-test"
                    :concurrency 1
                    :client (reify Client
                              (open! [this test node] this)
                              (setup! [this test])
                              (invoke! [this test op]
                                (assoc op :type :fail, :end-process? true))
                              (teardown! [this test])
                              (close! [this test]))
                    :generator (->> {:f :wag}
                                    (repeat 3)
                                    gen/clients))
        h (:history (jepsen/run! test))
        completions (h/remove h/invoke? h)]
    (is (= [[0 :fail :wag]
            [1 :fail :wag]
            [2 :fail :wag]]
           (map (juxt :process :type :f) completions)))))

(deftest run!-throw-test
  (testing "worker throws"
    (let [opens (atom 0)   ; Number of calls to open a client
          closes (atom 0)  ; Number of calls to close a client
          test (assoc base-test
                      :name "generator interpreter run!-throw-test"
                      :concurrency 1
                      :client (reify Client
                                (open! [this test node]
                                  (swap! opens inc)
                                  this)
                                (setup! [this test])
                                (invoke! [this test op]
                                  (assert false))
                                (teardown! [this test])
                                (close! [this test]
                                  (swap! closes inc)))
                      :nemesis  (reify Nemesis
                                  (setup! [this test] this)
                                  (invoke! [this test op] (assert false))
                                  (teardown! [this test]))
                      :generator
                      (->> (repeat 2 {:f :read})
                           (gen/nemesis
                             (repeat 2 {:type :info, :f :break}))))
          h           (:history (jepsen/run! test))
          completions (h/remove h/invoke? h)
          err "indeterminate: Assert failed: false"]
        (is (= [[:nemesis :info :break nil]
                [:nemesis :info :break err]
                [:nemesis :info :break nil]
                [:nemesis :info :break err]
                [0        :invoke :read nil]
                [0        :info   :read err]
                [1        :invoke :read nil]
                [1        :info   :read err]]
               (->> h
                    ; Try to cut past parallel nondeterminism
                    (sort-by :process util/poly-compare)
                    (map (juxt :process :type :f :error)))))
        ; We should have closed everything that we opened
        (is (= @opens @closes))))

    (testing "generator op throws"
      (let [call-count (atom 0)
            gen (->> (fn []
                       (swap! call-count inc)
                       (assert false))
                     (gen/limit 2))
            test (assoc base-test
                      :client     (ok-client)
                      :nemesis    (info-nemesis)
                      :generator  gen)
            e (try+ (:history (jepsen/run! test))
                    :nope
                    (catch [:type :jepsen.generator/op-threw] e e))]
        (is (= 1 @call-count))
        (is (= :jepsen.generator/op-threw (:type e)))
        (is (= (dissoc (datafy (gen/context test)) :time)
               (dissoc (:context e) :time)))))

    (testing "generator update throws"
      (let [gen (->> (reify gen/Generator
                       (op [this test ctx]
                         [(first (gen/op {:f :write, :value 2} test ctx))
                          this])

                       (update [this test ctx event]
                         (assert false)))
                     (gen/limit 2)
                     gen/validate
                     gen/friendly-exceptions)
            test (assoc base-test
                        :client (ok-client)
                        :nemesis (info-nemesis)
                        :generator gen)
            e (try+ (:history (jepsen/run! test))
                    :nope
                    (catch [:type :jepsen.generator/update-threw] e e))]
        (testing "context map"
          (let [expected-ctx (-> (datafy (gen/context test))
                                 (assoc :time (:time (:context e))
                                        :next-thread-index 1)
                                 (update :free-threads disj
                                         (:process (:event e))))]
            (is (= expected-ctx (:context e)))))
        (is (= (h/op {:index    0
                      :f        :write
                      :value    2
                      :time     (:time (:context e))
                      :process  (:process (:event e))
                      :type     :invoke})
               (:event e))))))
