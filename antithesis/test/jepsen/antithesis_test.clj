(ns jepsen.antithesis-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [antithesis :as a]
                    [checker :as checker]
                    [client :as client]
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

(deftest choices-test
  ; We redefine `jepsen.random`'s `long` and `double-weighted-index` so that they use the
  ; Antithesis randomChoice (for small choices) instead of weighted entropy.
  ; This should hopefully make Antithesis more efficient at exploring branches.
  ; Of course we have no way to TEST this outside Antithesis, since it just
  ; proxies back to j.u.Random, but we can at least make sure it's uniform
  ; rather than weighted.
  (let [weights (double-array [0.0 1.0])]
    (testing "stock"
      (dotimes [i 100]
        (is (== 1.0 (rand/double-weighted-index weights)))))

    (testing "antithesis"
      (with-redefs [a/antithesis? (constantly true)]
        (a/with-rng
          (testing "double-weighted-index"
            (let [freqs (->> #(rand/double-weighted-index weights)
                             repeatedly
                             (take 1000)
                             frequencies)]
              (is (= #{0 1} (set (keys freqs))))
              (is (< (Math/abs (- (freqs 0 0) (freqs 1 0))) 100))))

          (testing "long"
            ; Long's behavior should be uniform random either way, but we can
            ; at least check the range.
            (doseq [i (range 1 100)]
              (is (< (rand/long i) i))
              ; Just adding some offset (41) to check long's lower/upper bounds
              (is (< 40 (rand/long 41 (+ i 41)) (+ i 41)))))

          (testing "bool"
            ; Weighted booleans should stop being weighted when Antithesis takes over
            (let [freqs (->> #(rand/bool 1/10)
                             repeatedly
                             (take 1000)
                             frequencies)]
              (is (< (Math/abs (- (freqs true 0) (freqs false 0))) 100)))
            ; Unless their probability is extreme
            (let [freqs (->> #(rand/bool 1/100)
                             repeatedly
                             (take 1000)
                             frequencies)]
              (is (< (freqs true 0) 50)))))))))

(deftest client-test
  ; Clients should inform us they started, and also make assertions about
  ; invocations.
  (with-redefs [a/antithesis? (constantly true)]
    (let [side (atom [])
          client (reify client/Client
                   (open! [this test node]
                     (swap! side conj :open)
                     this)

                   (setup! [this test]
                     (swap! side conj :setup))

                   (invoke! [this test op]
                     (swap! side conj :invoke)
                     (assoc op :type :ok))

                   (teardown! [this test]
                     (swap! side conj :teardown))

                   (close! [this test]
                     (swap! side conj :close)))
          client' (a/client client)]
      ; Since assertions are macros, not sure how to test them.
      (let [sc! a/setup-complete!]
        (with-redefs [a/setup-complete! (fn
                                          ([] (a/setup-complete! nil))
                                          ([details] (swap! side conj :setup-complete) (sc! details)))]
          (let [client (client/open! client' {} "n1")]
            (is (= [:open] @side))
            (client/setup! client' {})
            (is (= [:open :setup :setup-complete] @side))
            (is (= :ok (:type (client/invoke! client' {} {:type :invoke}))))
            (client/teardown! client' {})
            (client/close! client {})
            (is (= [:open :setup :setup-complete :invoke :teardown :close]
                   @side))))))))

(deftest checker-test
  ; Checkers should assert validity. Again, I have no way to test this, since
  ; they're macros, but we can at least guarantee they proxy through.
  (with-redefs [a/antithesis? (constantly true)]
    (let [side (atom [])
          checker (reify checker/Checker
                    (check [this test history opts]
                      (swap! side conj :check)
                      {:valid? true}))
          checker' (a/checker checker)]
      (is (= {:valid? true} (checker/check checker' {} [] {})))
      (is (= [:check] @side)))))

(deftest checker+-test
  ; If we nest one checker in another via e.g. `compose`, each one should get a
  ; distinct wrapper.
  (with-redefs [a/antithesis? (constantly true)]
    (let [side (atom [])
          cat (reify checker/Checker
                (check [this test history opts]
                  (swap! side conj :cat)
                  {:type :cat, :valid? true}))

          dog (reify checker/Checker
                (check [this test history opts]
                  (swap! side conj :dog)
                  {:type :dog, :valid? :unknown}))

          checker (checker/compose {:cat cat, :dog dog})
          checker' (a/checker+ checker)
          ; We can ask for instance? a.Checker, but that breaks during hot code
          ; reload
          ac? (fn [x]
                (is (= "jepsen.antithesis.AlwaysChecker"
                       (-> x class .getName))))]
      ;(prn)
      ;(println "-----")
      ;(prn)
      ;(pprint checker')
      ; Obviously
      (is (ac? checker'))
      ; But also
      (is (ac? (-> checker' :checker :checkers :cat)))
      (is (ac? (-> checker' :checker :checkers :dog)))

      ; Paths should be nice and clean
      (is (= "checker :cat" (-> checker' :checker :checkers :cat :name)))
      (is (= "checker :dog" (-> checker' :checker :checkers :dog :name)))

      ; This should still work like a checker
      (is (= {:valid? :unknown
              :cat {:type :cat, :valid? true}
              :dog {:type :dog, :valid? :unknown}}
             (checker/check checker' {} [] {}))))))
