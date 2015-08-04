(ns jepsen.chronos.checker-test
  (:require [clj-time.core :as t]
            [jepsen.chronos.checker :refer :all]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]))

(deftest job->targets-test
  (testing "Late read"
    (is (= (job->targets (t/date-time 2001 1 1 0 0 0)
                         {:start (t/date-time 2000 1 1 0 0 0)
                          :count 3
                          :epsilon 10
                          :interval 60})
           [(t/interval (t/date-time 2000 1 1 0 0 0)
                        (t/date-time 2000 1 1 0 0 10))
            (t/interval (t/date-time 2000 1 1 0 1 0)
                        (t/date-time 2000 1 1 0 1 10))
            (t/interval (t/date-time 2000 1 1 0 2 0)
                        (t/date-time 2000 1 1 0 2 10))])))

  (testing "premature read"
    (is (= (job->targets (t/date-time 2000 1 1 0 1 10)
                         {:start (t/date-time 2000 1 1 0 0 0)
                          :count 3
                          :epsilon 10
                          :interval 60})
           [(t/interval (t/date-time 2000 1 1 0 0 0)
                        (t/date-time 2000 1 1 0 0 10))]))))

(deftest analyze-job-test
  (let [r1 {:start (t/date-time 2000 1 1 0 0 3)}
        r2 {:start (t/date-time 2000 1 1 0 1 5)}
        r3 {:start (t/date-time 2000 1 1 0 2 7)}
        r4 {:start (t/date-time 2000 1 1 0 3 0)}]
    (is (= (solution
             (t/date-time 2100 1 1 0 0 0)
             {:start (t/date-time 2000 1 1 0 0 0)
              :count 3
              :epsilon 10
              :interval 60}
             [r1 r2 r3 r4])
           {:valid? true
            :solution {(t/interval (t/date-time 2000 1 1 0 0 0)
                                   (t/date-time 2000 1 1 0 0 10)) r1
                       (t/interval (t/date-time 2000 1 1 0 1 0)
                                   (t/date-time 2000 1 1 0 1 10)) r2
                       (t/interval (t/date-time 2000 1 1 0 2 0)
                                   (t/date-time 2000 1 1 0 2 10)) r3}
            :extra     [r4]}))))
