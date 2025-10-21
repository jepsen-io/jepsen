(ns jepsen.random-test
  (:require [jepsen.random :as r]
            [clojure [pprint :refer [pprint]]
                     [string :as str]
                     [test :refer :all]]
            [criterium.core :refer [bench quick-bench]]))

(defmacro s
  "Sample. Evaluates expr n times, returning a vector of results."
  [n expr]
  `(loop [i#      ~n
          sample# (transient [])]
     (if (= i# 0)
       (persistent! sample#)
       (recur (dec i#)
              (conj! sample# ~expr)))))

(defmacro sfs
  "Sorted frequency sample. Takes a count n and an expression which generates a
  random value. Runs it n times, returning a sorted map of elements to
  frequencies."
  [n expr]
  `(loop [i#   ~n
          freqs# (sorted-map)]
    (if (= i# 0)
      freqs#
      (let [x# ~expr]
        (recur (dec i#)
               (assoc freqs# x# (inc (get freqs# x# 0))))))))

(defn mean
  "A bigdecimal mean of a collection"
  [xs]
  (double
    (/ (reduce + (map bigdec xs))
       (count xs))))

(defn range-
  "A vector of [min max]"
  [xs]
  [(reduce min xs) (reduce max xs)])

(defmacro demo
  "Demonstrates statistics of 100,000 runs of an expression; helpful for fooling around with RNGs"
  [expr]
  `(let [xs# (s 100000 ~expr)]
    (println (pr-str (quote ~expr)))
    (println "  mean:   " (mean xs#))
    (println "  range:  " (pr-str (range- xs#)))
    (println "  example:" (str/join " " (take 5 xs#)) "...\n")))

(deftest long-test
  (testing "no args"
    (r/with-seed 6
      (is (= [664015626436705906
              -4625650869307586295
              -7102489946756875200
              8952952060394960398
              7457574240327076316] (s 5 (r/long))))))

  (testing "upper bound"
    (r/with-seed 6
      (is (= [0 0 0] (s 3 (r/long 1)))))
    (r/with-seed 6
      (is (= [2 0 2 2 1 2] (s 6 (r/long 3))))))

  (testing "lower and upper bound"
    (r/with-seed 0
      (is (= [8 9 9 5 9 7] (s 6 (r/long 5 10)))))
    (r/with-seed -1
      (is (= {0 332, 1 317, 2 351}
             (sfs 1000 (r/long 3))))))

  (testing "uniform"
    (r/with-seed 8
      (let [sample (s 1000 (r/long -32 2))]
        (is (= -15.087 (mean sample)))))))

(deftest double-test
  (testing "no args"
    (r/with-seed 123
      (is (= [0.8824566406993949
              0.0010523092226565334
              0.3035720130282765
              0.3182820437647357
              0.11752272614269443]
             (s 5 (r/double)))))
    (let [sample (r/with-seed 123 (s 1000 (r/double)))]
      (testing "bounds"
        (is (every? #(and (<= 0 %) (< % 1)) sample)))
      (testing "mean"
        (is (= 0.5028828282787066
               (/ (reduce + sample) (count sample)))))))

  (testing "upper bound"
    (r/with-seed 123
      (is (= [28.238612502380636
              0.03367389512500907
              9.714304416904849
              10.185025400471542
              3.760727236566222]
             (s 5 (r/double 32)))))
    (let [sample (r/with-seed 123 (s 1000 (r/double 32)))]
      (testing "bounds"
        (is (every? #(and (<= 0 %) (< % 32)) sample)))
      (testing "mean"
        (is (= 16.09225050491861
               (/ (reduce + sample) (count sample)))))))

  (testing "lower and upper bound"
    (r/with-seed 2
      (is (= [-4.108154773507401
              -13.125797684438819
              0.6145475463877261
              -23.653905340845746
              -8.072434928951363]
             (s 5 (r/double -32 2)))))
    (let [sample (r/with-seed 123 (s 1000 (r/double -32 -2)))]
      (testing "bounds"
        (is (every? #(and (<= -32 %) (< % -2)) sample)))
      (testing "mean"
        (is (= -16.91351515163882
               (/ (reduce + sample) (count sample))))))))

(deftest exp-test
  (r/with-seed -5
    (let [xs (s 1000 (r/exp 3))]
      ; Always positive
      (testing "bounds"
        (is (= [0.004488780511024844 34.195839945461756] (range- xs))))

      ; Mean of a distribution is 1/lambda
      (testing "mean"
        (is (= 3.1918143056698303 (mean xs)))))))

(deftest geometric-test
  (r/with-seed 15
    (let [xs (s 1000 (r/geometric 1/3))]
			(testing "example"
        (is (= [1 0 0 2 0 2 2 3 0 0] (take 10 xs))))
			(testing "range"
				(is (= [0 19] (range- xs))))
			; mean should be (1-p)/p, which is (2/3)/(1/3) = 2
			(testing "mean"
  			(is (= 2.001 (mean xs)))))))

(deftest zipf-test
  (r/with-seed 13
    (let [xs (s 100000 (r/zipf 5))]
      ; With the default skew of ~1, we expect the first-rank element to appear
      ; roughly twice as often as the second, 3x as often as the third, 4x as
      ; often as the 4th, etc.
      (is (= {0 43762, 1 21869, 2 14753, 3 10852, 4 8764}
             (frequencies xs)))))

  (testing "skew 2"
    (r/with-seed 13
      ; With a skew of two, the first-rank element appears 4x as often as the
      ; second, 9x as often as the third, 16x as often as the 4th, etc.
      (let [xs (s 100000 (r/zipf 2 5))]
        (is (= {0 68267, 1 17214, 2 7515, 3 4277, 4 2727}
               (into (sorted-map) (frequencies xs))))))))

; Choosing elements

(deftest rand-nth-test
  (r/with-seed -6
		(let [xs (s 1000 (r/rand-nth [:a :b :c]))]
      (testing "example"
        (is (= [:b :c :c :c :c :b :c :c :b :a]
               (take 10 xs))))
      (testing "uniform"
  			(is (= {:a 343 :b 331 :c 326} (frequencies xs)))))))

(deftest rand-nth-empty-test
  (is (= nil (r/rand-nth-empty [])))
  (r/with-seed 124523
    (is (= :y (r/rand-nth-empty [:x :y])))))

(deftest double-weighted-index-test
  (is (= -1 (r/double-weighted-index  (double-array 0))))
  (is (= -1 (r/double-weighted-index  (double-array [0 0]))))
  (is (= 2 (r/double-weighted-index   (double-array [0 0 3 0]))))
  (is (#{0 3} (r/double-weighted-index (double-array [2 0 0 5]))))
  ; TODO: actually test distribution
  )

(deftest weighted-test
  (r/with-seed 506
    (let [xs (s 1000 (r/weighted {:x 1 :y 1/3 :z 2/3}))]
      (is (every? #{:x :y :z} xs))
      (is (= {:x 521 :y 165 :z 314}
             (frequencies xs))))))

(deftest shuffle-test
  (is (= [] (r/shuffle [])))
  (is (= () (r/shuffle ())))
  (r/with-seed 3
    (let [v  [1 2 3]
          xs (s 1000 (r/shuffle v))]
      (testing "example"
        (is (= [[3 2 1] [3 1 2] [1 2 3]]
               (take 3 xs))))
      (testing "size"
        (is (every? #{3} (map count xs))))
      (testing "elements"
        (is (every? #{#{1 2 3}} (map set xs)))))))

(deftest nonempty-subset-test
  (is (= nil (r/nonempty-subset [])))
  (r/with-seed -34567456
    (let [xs (s 1000 (r/nonempty-subset [:a :b :c]))]
      (testing "example"
        (is (= [[:c :a :b] [:a] [:c]]
               (take 3 xs))))
      (testing "uniformly distributed lengths"
        (is (= {3 300, 1 353, 2 347}
               (frequencies (map count xs)))))
      (testing "uniform elements"
        (is (= {:b 644, :a 654, :c 649}
               (frequencies (mapcat identity xs))))))))

; Perf

(deftest ^:perf rand-long-perf
  (println "# Bare Clojure rand-int")
  (quick-bench (rand-int 5))

  (println "# r/long")
  (println "## thread-local")
  (r/with-rng (r/thread-local-random)
    (quick-bench (r/long 5)))

  (println "## clojure.core")
  (r/with-rng (r/clojure-random)
    (quick-bench (r/long 5)))

  (println "## data.generators")
  (r/with-rng (r/data-generators-random)
    (quick-bench (r/long 5))))
