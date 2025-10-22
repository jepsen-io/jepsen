(ns jepsen.antithesis-test
  (:require [clojure.test :refer :all]
            [jepsen [antithesis :as a]
                    [random :as rand]]))

(deftest antithesis?-test
  (is (false? (a/antithesis?))))

(deftest random-test
  (testing "outside antithesis"
    (rand/with-seed 0
      (a/with-rng
        (is (= 3247545035024278667 (rand/long)))
        (is (= 0.07473821244042433 (rand/double))))))

  (testing "in antithesis"
    (with-redefs [a/antithesis? (constantly true)]
      (rand/with-seed 0
        (a/with-rng
          (testing "nondeterministic"
            ; That we *don't* get the with-seed 0 values means we're using
            ; antithesis.Random
            (is (not (= 3247545035024278667 (rand/long))))
            (is (not (= 0.07473821244042433 (rand/double)))))

          (testing "long"
            (testing "saturation"
              ; Make sure we're flipping every possible Long bit
              (loop [i   0
                     sat 0]
                (if (= i 1000)
                  (is (= -1 sat)) ; -1 is 2r111...111
                  (recur (inc i)
                         (bit-or sat (rand/long))))))

            (testing "upper"
              (dotimes [i 1000]
                (is (< (rand/long 5403) 5403))))

            (testing "lower, upper"
              (dotimes [i 1000]
                (let [x (rand/long -5 -2)]
                  (is (and (<= -5 x)
                           (< x -2)))))))

          (testing "double"
            (let [xs (vec (take 1000 (repeatedly rand/double)))]
              (testing "range"
                (is (every? #(<= 0.0 % 1.0) xs)))

              (testing "saturation"
                (let [bits (map #(Double/doubleToRawLongBits %) xs)]
                  ; Top two bits should be zero, otherwise we should saturate the
                  ; mantissa. The other leftmost bits are all 1.
                  ;(println (Long/toBinaryString (reduce bit-or 0 bits)))
                  (is (= (unsigned-bit-shift-right -1 2)
                         (reduce bit-or 0 bits)))))

              (testing "upper"
                (dotimes [i 1000]
                  (is (< (rand/double 5403) 5403))))

              (testing "lower, upper"
                (dotimes [i 1000]
                  (is (<= -10 (rand/double -5 -2) -2))))))
          )))))
