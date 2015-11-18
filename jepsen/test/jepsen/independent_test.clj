(ns jepsen.independent-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clojure.set :as set]
            [jepsen.independent :refer :all]
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

