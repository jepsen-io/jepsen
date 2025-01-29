(ns jepsen.chronos.checker-test
  (:require [clj-time.core :as t]
            [jepsen.chronos.checker :refer :all]
            [jepsen.checker :refer [check]]
            [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]))

(def ef epsilon-forgiveness)

(deftest job->targets-test
  (testing "Late read"
    (is (= (job->targets (t/date-time 2001 1 1 0 0 0)
                         {:start (t/date-time 2000 1 1 0 0 0)
                          :count 3
                          :epsilon 10
                          :interval 60
                          :duration 0})
           [[(t/date-time 2000 1 1 0 0 0)
             (t/date-time 2000 1 1 0 0 (+ ef 10))]
            [(t/date-time 2000 1 1 0 1 0)
             (t/date-time 2000 1 1 0 1 (+ ef 10))]
            [(t/date-time 2000 1 1 0 2 0)
             (t/date-time 2000 1 1 0 2 (+ ef 10))]])))

  (testing "premature read"
    (is (= (job->targets (t/date-time 2000 1 1 0 1 10)
                         {:start (t/date-time 2000 1 1 0 0 0)
                          :count 3
                          :epsilon 10
                          :interval 60
                          :duration 0})
           [[(t/date-time 2000 1 1 0 0 0)
             (t/date-time 2000 1 1 0 0 (+ ef 10))]]))))

(deftest job-solution-test
  (let [r1 {:start (t/date-time 2000 1 1 0 0 3)
            :end   (t/date-time 2000 1 1 0 0 13)}
        r2 {:start (t/date-time 2000 1 1 0 1 5)
            :end   (t/date-time 2000 1 1 0 1 15)}
        r3 {:start (t/date-time 2000 1 1 0 2 7)
            :end   (t/date-time 2000 1 1 0 2 17)}
        r4 {:start (t/date-time 2000 1 1 0 3 0)
            :end   (t/date-time 2000 1 1 0 3 10)}
        r5 {:start (t/date-time 2000 1 1 0 0 0)}
        job {:start (t/date-time 2000 1 1 0 0 0)
             :count 3
             :epsilon 10
             :interval 60
             :duration 0}]

    (testing "valid"
      (is (= (job-solution (t/date-time 2100 1 1 0 0 0) job [r1 r2 r3 r4 r5])
             {:valid?   true
              :job      job
              :solution {[(t/date-time 2000 1 1 0 0 0)
                          (t/date-time 2000 1 1 0 0 (+ ef 10))] r1
                         [(t/date-time 2000 1 1 0 1 0)
                          (t/date-time 2000 1 1 0 1 (+ ef 10))] r2
                         [(t/date-time 2000 1 1 0 2 0)
                          (t/date-time 2000 1 1 0 2 (+ ef 10))] r3}
              :extra      [r4]
              :complete   [r1 r2 r3 r4]
              :incomplete [r5]})))

    (testing "invalid"
      (is (= (job-solution (t/date-time 2100 1 1 0 0 0) job [r1 r3 r4 r5])
             {:valid?   false
              :job      job
              :solution {[(t/date-time 2000 1 1 0 0 0)
                          (t/date-time 2000 1 1 0 0 (+ ef 10))] r1
                         [(t/date-time 2000 1 1 0 1 0)
                          (t/date-time 2000 1 1 0 1 (+ ef 10))] nil
                         [(t/date-time 2000 1 1 0 2 0)
                          (t/date-time 2000 1 1 0 2 (+ ef 10))] r3}
              :extra      nil
              :complete   [r1 r3 r4]
              :incomplete [r5]})))))
