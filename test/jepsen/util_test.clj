(ns jepsen.util-test
  (:use clojure.test
        jepsen.util))

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
