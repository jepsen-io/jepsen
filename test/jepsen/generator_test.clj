(ns jepsen.generator-test
  (:use clojure.test)
  (:require [jepsen.generator :as gen]
            [clojure.set :as set]))

(def a-test {:nodes [:a :b :c :d :e]})

(defn ops
  "All ops from a generator"
  [gen]
  (let [ops (atom [])]
    (->> (range (count (:nodes a-test)))
         (map (fn [p] (future
                        (loop []
                          (when-let [op (gen/op gen a-test p)]
                            (swap! ops conj op)
                            (recur))))))
         doall
         (map deref)
         dorun)
    @ops))

(deftest complex-test
  (let [ops (ops (->> (gen/queue)
                      (gen/limit 100)
                      (gen/then (gen/once {:value :a}))
                      (gen/then (gen/once {:value :b}))
                      (gen/then (gen/once {:value :c}))
                      (gen/then (gen/once {:value :d}))))]
    (is (= 104 (count ops)))
    (is (= [:a :b :c :d] (map :value (take-last 4 ops))))

    (is (set/subset? (set (map :value ops))
                     (set (concat (range 0 99) [nil :a :b :c :d]))))))
