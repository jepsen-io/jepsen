(ns jepsen.load-test
  (:use clojure.test
        jepsen.load))

(defn most?
  "Like every, but fuzzy"
  [pred coll]
  (< 8/10 (/ (count (filter pred coll))
             (count coll))))

(deftest map-fixed-rate-test
  (testing "empty"
    (is (= (map-fixed-rate 10 inc [])
           [])))

  (testing "one"
    (is (= (map-fixed-rate 10 inc (list 1))
          [2])))

  (testing "more"
    (let [intervals (map-fixed-rate 1000
                                    (fn [x]
                                      [(System/currentTimeMillis)
                                       (do (Thread/sleep 10)
                                           (System/currentTimeMillis))])
                                    (range 1000))]
      ; All starting times are roughly 1 ms apart
      (let [starts (->> intervals
                        (map first)
                        (partition 2 1)
                        (map (comp - (partial apply -))))]
        (is (most? #{1} starts))
        (is (< 0.99 (/ (reduce + starts) (count starts)) 1.11)))

      ; All ending times are roughly 10ms after the start.
      (let [durations (map (comp - (partial apply -)) intervals)]
        (is (most? #{10} durations))
        (is (<= 9.9 (/ (reduce + durations) (count durations)) 10.1))))))

; (deftest map-fixed-rate-stress
;  (testing "lots"
;    (map-fixed-rate 1000
;                    (fn [i]
;                      (Thread/sleep 5000)
;                      (when (zero? (mod i 100))
;                        (prn i)))
;                    (range 100000))))
