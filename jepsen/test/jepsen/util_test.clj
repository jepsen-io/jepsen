(ns jepsen.util-test
  (:use clojure.test
        clojure.pprint
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

(deftest history->latencies-test
  (let [history
        [{:time 11457033239, :process 2, :type :invoke, :f :read}
         {:time 11457019103, :process 3, :type :invoke, :f :read}
         {:time 11457111283, :process 4, :type :invoke, :f :cas, :value [0 2]}
         {:time 11457094604, :process 0, :type :invoke, :f :cas, :value [4 4]}
         {:time 11457159210, :process 1, :type :invoke, :f :cas, :value [3 1]}
         {:value nil, :time 11473961208, :process 2, :type :ok, :f :read}
         {:value nil, :time 11473953899, :process 3, :type :ok, :f :read}
         {:time 11478831184, :process 4, :type :info, :f :cas, :value [0 2]}
         {:time 11478852616, :process 1, :type :fail, :f :cas, :value [3 1]}
         {:time 11478859479, :process 0, :type :fail, :f :cas, :value [4 4]}
         {:time 12475010505, :process 2, :type :invoke, :f :read}
         {:time 12475010560, :process :nem :type :info :f :hi}
         {:time 12475232472, :process 3, :type :invoke, :f :write, :value 0}
         {:value nil, :time 12477011002, :process 2, :type :ok, :f :read}
         {:time 12479523408, :process 4, :type :invoke, :f :cas, :value [1 0]}
         {:time 12479572112, :process 0, :type :invoke, :f :write, :value 1}
         {:time 12479552107, :process 1, :type :invoke, :f :cas, :value [4 3]}
         {:time 12480010179, :process 3, :type :ok, :f :write, :value 0}
         {:time 12481345684, :process 1, :type :fail, :f :cas, :value [4 3]}
         {:time 12484071466, :process 0, :type :ok, :f :write, :value 1}
         {:time 12484388730, :process 4, :type :ok, :f :cas, :value [1 0]}]
        h    (history->latencies history)
        n->m (partial * 1e-6)]
    (->> h
         (filter #(= :invoke (:type %)))
         (map (juxt (comp n->m :time)
                    (comp n->m :latency)))
;         (map (fn [[time latency]]
;                (println (str time "," latency))))
         ;TODO: actually assert something
         dorun)))

(deftest longest-common-prefix-test
  (is (= nil (longest-common-prefix [])))
  (is (= [] (longest-common-prefix [[1 2] [3 4]])))
  (is (= [1 2] (longest-common-prefix [[1 2]])))
  (is (= [1 2 3] (longest-common-prefix [[1 2 3] [1 2 3 4] [1 2 3 6]]))))

(deftest drop-common-proper-prefix-test
  (is (= [[3 4] [5 6]] (drop-common-proper-prefix [[1 3 4] [1 5 6]])))
  (is (= [[1]] (drop-common-proper-prefix [[1]])))
  (is (= [[2]] (drop-common-proper-prefix [[1 2]])))
  (is (= [[2] [2]] (drop-common-proper-prefix [[1 2] [1 2]]))))

(deftest letr-test
  (testing "no bindings"
    (is (= (letr []) nil))
    (is (= (letr [] 1 2) 2)))

  (testing "standard bindings"
    (is (= (letr [a 1, b a] 2 a) 1)))

  (testing "early return"
    (let [side-effect (atom false)]
      (is (= (letr [a   1
                    x   (if (pos? a) (return :pos) :neg)
                    foo (reset! side-effect true)]
               x)
             :pos))
      (is (not @side-effect))))

  (testing "using non-return branch"
    (let [side-effect (atom false)]
      (is (= (letr [a   -1
                    x   (if (pos? a) (return :pos) :neg)
                    foo (reset! side-effect true)]
               x)
             :neg))
      (is @side-effect)))

  (testing "multiple return"
    (is (= (letr [a 2
                  _ (when (= a 1) (return :1))
                  _ (when (= a 2) (return :2))
                  _ (when (= a 3) (return :3))]
             4)
           :2))))

(deftest timeout-test
  ; Fast operations pass through the inner result or exception.
  (is ::success (timeout 1000 ::timed-out
                         ::success))
  (is (thrown? ArithmeticException
               (timeout 1000 ::timed-out
                        (/ 1 0))))
  ; Slow operations are interrupted and return timeout value.
  (is ::timed-out (timeout 10 ::timed-out
                           (Thread/sleep 1000)))
  ; This is a more complicated version of the previous test that
  ; verifies that the function is interrupted when a timeout occurs.
  (let* [p (promise)
         ret (timeout 10 ::timed-out
                      (try
                        (Thread/sleep 1000)
                        (deliver p ::finished)
                        (catch InterruptedException e
                          (deliver p ::exception))))]
    (is ::timed-out ret)
    (is ::exception (deref p 10 ::timed-out))))
