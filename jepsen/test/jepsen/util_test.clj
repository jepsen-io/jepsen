(ns jepsen.util-test
  (:use clojure.test
        jepsen.util))

(deftest majority-test
  (is (= 1 (majority 0)))
  (is (= 1 (majority 1)))
  (is (= 2 (majority 2)))
  (is (= 2 (majority 3)))
  (is (= 3 (majority 4)))
  (is (= 3 (majority 5))))

(deftest integer-interval-set-str-test
  (is (= (integer-interval-set-str [])
         "#{}"))

  (is (= (integer-interval-set-str [1])
         "#{1}"))

  (is (= (integer-interval-set-str [1 2])
         "#{1..2}"))

  (is (= (integer-interval-set-str [1 2 3])
         "#{1..3}"))

  (is (= (integer-interval-set-str [1 3 5])
         "#{1 3 5}"))

  (is (= (integer-interval-set-str [1 2 3 5 7 8 9])
         "#{1..3 5 7..9}")))
