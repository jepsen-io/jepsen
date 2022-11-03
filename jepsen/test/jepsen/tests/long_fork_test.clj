(ns jepsen.tests.long-fork-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [jepsen [checker :as checker]
                    [history :as h]]
            [jepsen.tests.long-fork :refer :all]))

(deftest checker-test
  (let [; r1 sees 1 1 nil
        r1  {:type :invoke, :f :read, :value [[:r 0 nil] [:r 1 nil] [:r 2 nil]]}
        r1' {:type :ok, :f :read, :value [[:r 0 1] [:r 1 1] [:r 2 nil]]}
        ; r2 sees 1 nil 1
        r2  {:type :invoke, :f :read, :value [[:r 2 nil] [:r 0 nil] [:r 1 nil]]}
        r2' {:type :ok, :f :read, :value [[:r 2 1]   [:r 0 1]   [:r 1 nil]]}
        h (h/history [r1 r2 r1' r2'])
        [r1 r2 r1' r2'] h]
    (is (= {:valid? false
            :early-read-count 0
            :late-read-count 0
            :reads-count 2
            :forks [[r1' r2']]}
           (checker/check (checker 3) {} h {})))))
