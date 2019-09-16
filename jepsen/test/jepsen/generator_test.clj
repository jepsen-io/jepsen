(ns jepsen.generator-test
  (:use clojure.test
        clojure.pprint
        clojure.tools.logging)
  (:require [jepsen [common-test :refer [quiet-logging]]
                    [generator :as gen]]
            [tea-time [core :as tt]]
            [clojure.set :as set]))

(use-fixtures :once quiet-logging)

(def nodes [:a :b :c :d :e])
(def a-test {:nodes nodes})

(defn ops
  "All ops from a generator"
  [threads gen]
  (let [ops (atom [])
        t   (assoc a-test :concurrency (count (filter integer? threads)))]
    (binding [gen/*threads* threads]
      (->> threads
           (map (fn [p] (future
                          (loop []
                            (when-let [op (gen/op gen t p)]
                              (swap! ops conj op)
                              (recur))))))
           doall
           (map deref)
           dorun))
    @ops))

(deftest object-as-generators
  (is (= (gen/op 2 a-test 1) 2))
  (is (= (gen/op {:foo 2} a-test 1) {:foo 2})))

(deftest fns-as-generators
  (is (= (gen/op (fn [a b] [a b]) :test :process) [:test :process])))

(deftest seq-test
  (is (= (set (ops (:nodes a-test)
                   (gen/seq (range 100))))
         (set (range 100)))))

(deftest seq-all-test
  (tt/with-threadpool
    (testing "fixed sequences"
      (let [ops (ops [1] (gen/seq-all [(gen/limit 2 :a)
                                       (gen/limit 3 :b)]))]
        (is (= [:a :a :b :b :b] ops))))

    (testing "complex"
      (let [busy  #(gen/limit 2 (gen/delay-til 0.1 :x))
            quiet #(gen/phases (gen/once :shh)
                               (gen/sleep 0.2))
            gen  (gen/time-limit 1 (gen/seq-all
                                     (interleave (repeatedly busy)
                                                 (repeatedly quiet))))
            ops   (ops [1] gen)]
        (is (= [:x :x :shh :x :x :shh :x :x] ops))))))

(deftest complex-test
  (let [ops (ops (:nodes a-test)
                 (->> (gen/queue)
                      (gen/limit 100)
                      (gen/then (gen/once {:value :a}))
                      (gen/then (gen/once {:value :b}))
                      (gen/then (gen/once {:value :c}))
                      (gen/then (gen/once {:value :d}))))]
    (is (= 104 (count ops)))
    (is (= [:a :b :c :d] (map :value (take-last 4 ops))))

    (is (set/subset? (set (map :value ops))
                     (set (concat (range 0 99) [nil :a :b :c :d]))))))

(deftest log-test
  (let [ops (ops (:nodes a-test)
                 (gen/phases (gen/log "start")
                             (gen/limit (count (:nodes a-test))
                                        {:value :hi})
                             (gen/log "stop")))]
    (is (= ops (repeat (count (:nodes a-test)) {:value :hi})))))

(deftest then-test
  (testing "phases and then"
    (is (= (ops (:nodes a-test)
                (gen/phases
                  (gen/on #{:c :d}
                          (->> (gen/once 1)
                               (gen/then (gen/once 2))))))
           [1 2]))))

(deftest each-test
  (is (= (ops (:nodes a-test)
              (gen/each (gen/once :a)))
         [:a :a :a :a :a])))

(deftest nemesis-phase-test
  (testing "nemesis can take part in synchronization barriers"
    (is (= (ops (cons :nemesis (:nodes a-test))
                (gen/phases (gen/once :a)
                            (gen/once :b)))
           [:a :b])))

  (testing "nemesis is not included when we filter generator processes"
    (is (= (ops (cons :nemesis (:nodes a-test))
                (gen/phases (->> (gen/once :start)
                                 (gen/nemesis (gen/once :start)))
                            (gen/nemesis (gen/once :nem))
                            (gen/on (complement #{:nemesis})
                                    (gen/synchronize
                                      (gen/each (gen/once :*))))
                            (gen/on #{:c :d}
                                    (->> (gen/once :c)
                                         (gen/then (gen/once :d))))))
           [:start :start :nem
            :* :* :* :* :*
            :c :d]))))

(deftest time-limit-test
  (tt/with-threadpool
    (testing "short delays"
      (let [ops (ops nodes
                     (->> (gen/seq (range))
                          (gen/delay 0.1)
                          (gen/time-limit 1)))
            n (* (count nodes) (/ 1 0.1))]
        (is (<= (* 0.9 n) (count ops) (* 1.1 n)))))

    (testing "long delays"
      (let [t1  (tt/unix-time)
            ops (ops nodes
                     (->> (gen/seq (range))
                          (gen/delay 1)
                          (gen/time-limit 0.1)))
            t2  (tt/unix-time)]
        (is (= [] ops))
        (is (< 0.09 (- t2 t1) 0.11))))

    (testing "long inside short"
      (let [t1  (tt/unix-time)
            ops (ops nodes
                     (->> (gen/seq (range))
                          (gen/delay 0.15)
                          (gen/time-limit 10)
                          (gen/time-limit 0.2)))
            t2  (tt/unix-time)]
        (is (= (range (count nodes)) (sort ops)))
        (is (<= 0.19 (- t2 t1) 0.21))))

    (testing "short inside long"
      (let [t1  (tt/unix-time)
            ops (->> (gen/seq (range))
                     (gen/delay 0.15)
                     (gen/time-limit 0.2)
                     (gen/time-limit 10)
                     (ops nodes))
            t2  (tt/unix-time)]
        (is (= (range (count nodes)) (sort ops)))
        (is (<= 0.19 (- t2 t1) 0.21))))

    (testing "around a barrier"
      (let [t1 (tt/unix-time)
            ops (->> (gen/phases
                       (gen/delay 0.1 (gen/each (gen/once :a)))
                       (gen/delay 1   :b))
                     (gen/time-limit 0.2)
                     (ops nodes))
            t2 (tt/unix-time)]
        (is (= (repeat (count nodes) :a) ops))
        (is (<= 0.19 (- t2 t1) 0.21))))))
