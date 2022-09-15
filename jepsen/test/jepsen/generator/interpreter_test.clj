(ns jepsen.generator.interpreter-test
  (:refer-clojure :exclude [run!])
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.generator :as gen]
            [jepsen.generator.interpreter :refer :all]
            [jepsen [client :refer [Client]]
                    [nemesis :refer [Nemesis]]
                    [util :as util]]
            [knossos.op :as op]
            [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def base-test
  {:nodes  ["n1" "n2" "n3" "n4" "n5"]
   :concurrency 10})

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

(deftest run!-test
  (let [time-limit     1
        sleep-duration 1/10
        test (assoc base-test
              :client (reify Client
                        (open! [this test node] this)
                        (setup! [this test])
                        (invoke! [this test op]
                          ; We actually have to sleep here, or else it runs so
                          ; fast that reserve starves some threads.
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
                (gen/log "Done recovering; final read")
                (gen/clients (gen/until-ok (repeat {:f :read})))))
        h    (util/with-relative-time (run! test))
        nemesis-ops (filter (comp #{:nemesis} :process) h)
        client-ops  (remove (comp #{:nemesis} :process) h)]

    (testing "general structure"
      (is (vector? h))
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
      ;(prn (float (/ (count h) time-limit)))
      (is (< 5000 (float (/ (count h) time-limit)))))
    ))

(deftest run!-end-process-test
  ; When a client explicitly signifies that it'd like to end the process, we
  ; should spawn a new one.
  (let [test (assoc base-test
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
        h (util/with-relative-time (run! test))
        completions (remove op/invoke? h)]
    (is (= [[0 :fail :wag]
            [1 :fail :wag]
            [2 :fail :wag]]
           (map (juxt :process :type :f) completions)))))

(deftest run!-throw-test
  (testing "worker throws"
    (let [test (assoc base-test
                      :concurrency 1
                      :client (reify Client
                                (open! [this test node] this)
                                (setup! [this test])
                                (invoke! [this test op] (assert false))
                                (teardown! [this test])
                                (close! [this test]))
                      :nemesis  (reify Nemesis
                                  (setup! [this test] this)
                                  (invoke! [this test op] (assert false))
                                  (teardown! [this test]))
                      :generator
                      (->> (repeat 2 {:f :read})
                           (gen/nemesis
                             (repeat 2 {:type :info, :f :break}))))
          h           (util/with-relative-time (run! test))
          completions (remove op/invoke? h)
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
                    (map (juxt :process :type :f :error)))))))

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
            e (try+ (util/with-relative-time (run! test))
                    :nope
                    (catch [:type :jepsen.generator/op-threw] e e))]
        (is (= 1 @call-count))
        (is (= :jepsen.generator/op-threw (:type e)))
        (is (= (dissoc (gen/context test) :time)
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
            e (try+ (util/with-relative-time (run! test))
                    :nope
                    (catch [:type :jepsen.generator/update-threw] e e))]
        (is (= (let [ctx (gen/context test)]
                 (assoc ctx
                        :time     (:time (:context e))
                        :free-threads (.remove (:free-threads ctx) (:process (:event e)))))
               (:context e)))
        (is (= {:f        :write
                :value    2
                :time     (:time (:context e))
                :process  (:process (:event e))
                :type     :invoke}
               (:event e))))))
