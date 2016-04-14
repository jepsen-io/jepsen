(ns jepsen.independent-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [jepsen.independent :refer :all]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.generator-test :as gen-test :refer [ops]]))

(deftest sequential-generator-test
  (testing "empty keys"
    (is (= []
           (ops [:a :b] (sequential-generator [] (fn [k] :x))))))

  (testing "one key"
    (is (= [{:value [:k1 :ashley]}
            {:value [:k1 :katchadourian]}]
           (ops [:a] (sequential-generator
                       [:k1]
                       (fn [k] (gen/seq [{:value :ashley}
                                         {:value :katchadourian}])))))))

  (testing "n keys"
    (is (= [[1 0]
            [2 0]
            [2 1]
            [3 0]
            [3 1]
            [3 2]]
           (->> (fn [k] (gen/seq (map (partial array-map :value) (range k))))
                (sequential-generator [1 2 3])
                (ops [:a])
                (map :value)))))

  (testing "concurrency"
    (let [kmax 100
          vmax 100]
      (is (= (set (for [k (range kmax), v (range vmax)] [k v]))
             (->> (fn [k] (gen/seq (map (partial array-map :value)
                                        (range vmax))))
                  (sequential-generator (range kmax))
                  (ops (range 10))
                  (map :value)
                  set))))))

(deftest checker-test
  (let [even-checker (reify checker/Checker
                       (check [this test model history opts]
                         {:valid? (even? (count history))}))
        history (->> (fn [k] (->> (range k)
                                  (map (partial array-map :value))
                                  gen/seq))
                     (sequential-generator [0 1 2 3])
                     (ops [:a :b :c])
                     (concat [{:value :not-sharded}]))]
    (is (= {:valid? false
            :results {1 {:valid? true}
                      2 {:valid? false}
                      3 {:valid? true}}
            :failures [2]}
           (checker/check (checker even-checker)
                          {:name "independent-checker-test"
                           :start-time 0}
                          nil
                          history
                          {})))))
