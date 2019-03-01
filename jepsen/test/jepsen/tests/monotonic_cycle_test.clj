(ns jepsen.tests.monotonic-cycle-test
  (:require [jepsen.tests.monotonic-cycle :refer :all]
            [jepsen.checker :as checker]
            [knossos.op :as op]
            [clojure.test :refer :all]))

(def numeric-graph
  {1 #{2},
   2 #{3},
   3 #{1},
   4 #{2 3 5},
   5 #{4 6},
   6 #{3 7},
   7 #{6},
   8 #{7 8}})

(def key-graph
  {:a #{:b},
   :b #{:c},
   :c #{:a},
   :d #{:b :c :e},
   :e #{:d :f},
   :f #{:c :g},
   :g #{:f},
   :h #{:f :h}})

(deftest tarjan-test
  (testing "Returns strongly connected components"
    ;; Graph is taken from the wikipedia page
    (is (= (tarjan (keys key-graph) key-graph)
           #{#{:c :b :a} #{:g :f} #{:e :d} #{:h}}))))

(deftest cycle-test
  (testing "Can flag cyclic reads"
    (let [c (checker)
          history [(op/invoke 0 :read nil)
                   (op/ok     0 :read [0 1])
                   (op/invoke 0 :read nil)
                   (op/ok     0 :read [1 0])]
          r (checker/check checker nil history nil)]
      (is (not (:valid? r))))))
