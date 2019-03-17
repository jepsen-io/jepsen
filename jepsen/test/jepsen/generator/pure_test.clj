(ns jepsen.generator.pure-test
  (:require [jepsen.generator.pure :as gen]
            [clojure.test :refer :all]))

(def default-test
  "A default test map."
  {:concurrency 3})

(def default-context
  "A default initial context for running these tests. Two worker threads, one
  nemesis."
  {:time 0
   :free-processes [0 1 :nemesis]
   :workers {0 0
             1 1
             :nemesis :nemesis}})

(defn quick-ops
  "Simulates the series of ops obtained from a generator where the
  system executes every operation perfectly, immediately, and with zero
  latency."
  [gen]
  (loop [ops []
         gen (gen/validate gen)
         ctx default-context]
    (let [[invocation gen] (gen/op gen default-test ctx)]
      (condp = invocation
        nil ops ; Done!

        :pending (assert false "Uh, we're not supposed to be here")

        (let [; Advance clock
              ctx'       (update ctx :time max (:time invocation))
              ; Update generator
              gen'       (gen/update gen default-test ctx' invocation)
              ; Pretend to do operation
              completion (assoc invocation :type :ok)
              ; Advance clock to completion
              ctx''      (update ctx' :time max (:time completion))
              ; And update generator
              gen''      (gen/update gen' default-test ctx'' completion)]
          (recur (conj ops invocation completion)
                 gen''
                 ctx''))))))

(defn invocations
  "Only invokes, not returns"
  [history]
  (filter #(= :invoke (:type %)) history))

(defn quick
  "Like quick-ops, but returns just invocations."
  [gen]
  (invocations (quick-ops gen)))

(deftest nil-test
  (is (= [] (quick nil))))

(deftest map-test
  (is (= [{:time 0
          :process 0
          :type :invoke
          :f :write}]
         (quick (gen/once {:f :write}))))

  (testing "all threads busy"
    (is (= [:pending {:f :write}]
           (gen/op {:f :write} {} (assoc default-context
                                         :free-processes []))))))

(deftest limit-test
  (is (= [{:type :invoke :process 0 :time 0 :f :write :value 1}
          {:type :invoke :process 0 :time 0 :f :write :value 1}]
         (->> {:f :write :value 1}
              (gen/limit 2)
              quick))))

(deftest delay-til-test
  (is (= [{:type :invoke, :process 0, :time 0, :f :write}
          {:type :invoke, :process 0, :time 2, :f :write}
          {:type :invoke, :process 0, :time 4, :f :write}]
         (->> {:f :write}
              (gen/delay-til 2e-9)
              (gen/limit 3)
              quick))))

(deftest fn-test
  (testing "returning nil"
    (is (= [] (quick (fn [])))))

  (testing "returning pairs of [op gen']"
    ; This function constructs a map with the given value, and returns a
    ; successive generator which calls itself with that value, decremented.
    ; This is a weird thing to do, but I think it tests the core behavior.
    (letfn [(countdown [x test ctx] (when (pos? x)
                             [{:type    :invoke
                               :process (first (:free-processes ctx))
                               :time    (:time ctx)
                               :value   x}
                              (partial countdown (dec x))]))]
      (is (= [5 4 3 2 1]
             (->> (partial countdown 5)
                  quick
                  (map :value))))))

  (testing "returning maps"
    (let [ops (->> (fn [] {:f :write, :value (rand-int 10)})
                   (gen/limit 5)
                   quick)]
      (is (= 5 (count ops)))                      ; limit
      (is (every? #(<= 0 % 10) (map :value ops))) ; random vals
      (is (< 1 (count (set (map :value ops)))))   ; random vals
      (is (every? #{0} (map :process ops))))))    ; processes assigned
