(ns jepsen.generator.pure-test
  (:require [jepsen.generator.pure :as gen]
            [jepsen [util :as util]]
            [clojure.test :refer :all]))

(def default-test
  "A default test map."
  {})

(defn n+nemesis-context
  "A context with n numeric worker threads and one nemesis."
  [n]
  {:time 0
   :free-threads (into #{:nemesis} (range n))
   :workers (->> (range n)
                 (cons :nemesis)
                 (map (partial repeat 2))
                 (map vec)
                 (into {}))})

(def default-context
  "A default initial context for running these tests. Two worker threads, one
  nemesis."
  (n+nemesis-context 2))

(defn invocations
  "Only invokes, not returns"
  [history]
  (filter #(= :invoke (:type %)) history))

(defn quick-ops
  "Simulates the series of ops obtained from a generator where the
  system executes every operation perfectly, immediately, and with zero
  latency."
  ([gen]
   (quick-ops default-context gen))
  ([ctx gen]
   (loop [ops []
          gen (gen/validate gen)
          ctx ctx]
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
                  ctx'')))))))

(defn quick
  "Like quick-ops, but returns just invocations."
  ([gen]
   (quick default-context gen))
  ([ctx gen]
   (invocations (quick-ops ctx gen))))

(defn simulate
  "Simulates the series of operations obtained from a generator, given a
  function that takes ops and returns their completions."
  ([gen complete-fn]
   (simulate default-context gen complete-fn))
  ([ctx gen complete-fn]
   (loop [ops        []
          in-flight  [] ; Kept sorted by time
          gen        (gen/validate gen)
          ctx        ctx]
     (let [[invoke gen'] (gen/op gen default-test ctx)]
       ;(prn :invoke invoke :in-flight in-flight)
       (if (nil? invoke)
         ; We're done
         (into ops in-flight)

         ; TODO: the order of updates for worker maps here isn't correct; fix
         ; it.
         (if (and (not= :pending invoke)
                  (or (empty? in-flight)
                      (<= (:time invoke) (:time (first in-flight)))))

           ; We have an invocation that's not pending, and that invocation is
           ; before every in-flight completion
           (let [thread    (gen/process->thread ctx (:process invoke))
                 ; Advance clock, mark thread as free
                 ctx       (-> ctx
                               (update :time max (:time invoke))
                               (update :free-threads disj thread))
                 ; Update the generator with this invocation
                 gen'      (gen/update gen' default-test ctx invoke)
                 ; Add the completion to the in-flight set
                 ;_         (prn :invoke invoke)
                 complete  (complete-fn invoke)
                 in-flight (sort-by :time (conj in-flight complete))]
             (recur (conj ops invoke) in-flight gen' ctx))

           ; We need to complete something before we can apply the next
           ; invocation.
           (let [op     (first in-flight)
                 _      (assert op "generator pending and nothing in flight???")
                 thread (gen/process->thread ctx (:process op))
                 ; Advance clock, mark thread as free
                 ctx    (-> ctx
                            (update :time max (:time op))
                            (update :free-threads conj thread))
                 ; Update generator with completion
                 gen'   (gen/update gen default-test ctx op)
                 ; Update worker mapping if this op crashed
                 ctx    (if (or (= :nemesis thread) (not= :info (:type op)))
                          ctx
                          (update ctx :workers
                                  assoc thread (gen/next-process ctx thread)))]
             (recur (conj ops op) (rest in-flight) gen' ctx))))))))

(def perfect-latency
  "How long perfect operations take"
  10)

(defn perfect
  "Simulates the series of ops obtained from a generator where the system
  executes every operation successfully in 10 nanoseconds. Returns only
  invocations."
  ([gen]
   (perfect default-context gen))
  ([ctx gen]
   (invocations
     (simulate ctx gen
               (fn [invoke]
                 (-> invoke
                     (assoc :type :ok)
                     (update :time + perfect-latency)))))))

(defn perfect-info
  "Simulates the series of ops obtained from a generator where every operation
  crashes with :info in 10 nanoseconds. Returns only invocations."
  ([gen]
   (perfect-info default-context gen))
  ([ctx gen]
   (invocations
     (simulate ctx gen
               (fn [invoke]
                 (-> invoke
                     (assoc :type :info)
                     (update :time + perfect-latency)))))))

(deftest nil-test
  (is (= [] (perfect nil))))

(deftest map-test
  (testing "once"
    (is (= [{:time 0
             :process 0
             :type :invoke
             :f :write}]
           (perfect (gen/once {:f :write})))))

  (testing "concurrent"
    (is (= [{:type :invoke, :process 0, :f :write, :time 0}
            {:type :invoke, :process 1, :f :write, :time 0}
            {:type :invoke, :process :nemesis, :f :write, :time 0}
            {:type :invoke, :process :nemesis, :f :write, :time 10}
            {:type :invoke, :process 1, :f :write, :time 10}
            {:type :invoke, :process 0, :f :write, :time 10}]
           (perfect (gen/limit 6 {:f :write})))))

  (testing "all threads busy"
    (is (= [:pending {:f :write}]
           (gen/op {:f :write} {} (assoc default-context
                                         :free-threads []))))))

(deftest limit-test
  (is (= [{:type :invoke :process 0 :time 0 :f :write :value 1}
          {:type :invoke :process 0 :time 0 :f :write :value 1}]
         (->> {:f :write :value 1}
              (gen/limit 2)
              quick))))


(deftest delay-til-test
  (is (= [{:type :invoke, :process 0, :time 0, :f :write}
          {:type :invoke, :process 1, :time 0, :f :write}
          {:type :invoke, :process :nemesis, :time 0, :f :write}
          {:type :invoke, :process 0, :time 12, :f :write}
          {:type :invoke, :process 1, :time 12, :f :write}]
          (->> {:f :write}
              (gen/delay-til 3e-9)
              (gen/limit 5)
              perfect))))


(deftest seq-test
  (testing "vectors"
    (is (= [1 2 3]
           (->> [(gen/once {:value 1})
                 (gen/once {:value 2})
                 (gen/once {:value 3})]
                quick
                (map :value)))))

  (testing "seqs"
    (is (= [1 2 3]
           (->> [{:value 1}
                 {:value 2}
                 {:value 3}]
                (map gen/once)
                quick
                (map :value))))))

(deftest fn-test
  (testing "returning nil"
    (is (= [] (quick (fn [])))))

  (testing "returning pairs of [op gen']"
    ; This function constructs a map with the given value, and returns a
    ; successive generator which calls itself with that value, decremented.
    ; This is a weird thing to do, but I think it tests the core behavior.
    (letfn [(countdown [x test ctx] (when (pos? x)
                             [{:type    :invoke
                               :process (first (gen/free-processes ctx))
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

(deftest synchronize-test
  (is (= [{:f :a, :process 0, :time 2, :type :invoke}
          {:f :a, :process 1, :time 3, :type :invoke}
          {:f :a, :process :nemesis, :time 5, :type :invoke}
          {:f :b, :process 0, :time 15, :type :invoke}
          {:f :b, :process 1, :time 15, :type :invoke}]
         (->> [(->> (fn [test ctx]
                      (let [p     (first (gen/free-processes ctx))
                            ; This is technically illegal: we should return the
                            ; NEXT event by time. We're relying on the specific
                            ; order we get called here to do this. Fragile hack!
                            delay (case p
                                    0        2
                                    1        1
                                    :nemesis 2)]
                        {:f :a, :process p, :time (+ (:time ctx) delay)}))
                    (gen/limit 3))
               ; The latest process, the nemesis, should start at time 5 and
               ; finish at 15.
               (gen/synchronize (gen/limit 2 {:f :b}))]
              perfect))))

(deftest clients-test
  (is (= #{0 1}
         (->> {}
              (gen/clients)
              (gen/limit 5)
              perfect
              (map :process)
              set))))

(deftest phases-test
  (is (= [[:a 0 0]
          [:a 1 0]
          [:b 0 10]
          [:c 0 20]
          [:c 1 20]
          [:c 1 30]]
         (->> (gen/phases (gen/limit 2 {:f :a})
                          (gen/limit 1 {:f :b})
                          (gen/limit 3 {:f :c}))
              gen/clients
              perfect
              (map (juxt :f :process :time))))))

(deftest any-test
  ; We take two generators, each of which is restricted to a single process,
  ; and each of which takes time to schedule. When we bind them together with
  ; Any, they can interleave.
  (is (= [[:a 0 0]
          [:b 1 0]
          [:a 0 20]
          [:b 1 20]]
         (->> (gen/any (gen/on #{0} (gen/delay-til 20e-9 {:f :a}))
                       (gen/on #{1} (gen/delay-til 20e-9 {:f :b})))
              (gen/limit 4)
              perfect
              (map (juxt :f :process :time))))))

(deftest each-thread-test
  (is (= [[0 0 :a]
          [0 1 :a]
          [0 :nemesis :a]
          [10 :nemesis :b]
          [10 1 :b]
          [10 0 :b]]
         ; Each thread now gets to evaluate [a b] independently.
         (->> (gen/each-thread (map gen/once [{:f :a} {:f :b}]))
              perfect
              (map (juxt :time :process :f))))))

(deftest stagger-test
  (let [n           1000
        dt          20
        concurrency (count (:workers default-context))
        times       (->> (range n)
                         (map (fn [x] {:f :write, :value x}))
                         (map gen/once)
                         (gen/stagger (util/nanos->secs dt))
                         perfect
                         (mapv :time))
        max-time    (peek times)
        rate        (/ n max-time)

        ; How long do we spend waiting and working on a single op, on avg?
        t-wait dt
        t-work perfect-latency

        ; Work happens concurrently
        expected-work-time (-> perfect-latency (* n) (/ concurrency))

        ; Waiting happens sequentially
        expected-wait-time (* dt n)

        ; And this is how long the whole wait process should take. This isn't
        ; right when wait time is on the order of work time <sigh>.
        expected-time (long (+ expected-wait-time expected-work-time))]

    ; Sigh, throw away all that work and just hard-code these limits.
    (is (< 0.035 rate 0.040))))

(deftest f-map-test
  (is (= [{:type :invoke, :process 0, :time 0, :f :b, :value 2}]
         (->> {:f :a, :value 2}
              (gen/f-map {:a :b})
              gen/once
              perfect))))

(deftest filter-test
  (is (= [0 2 4 6 8]
         (->> (range)
              (map (fn [x] {:value x}))
              (map gen/once)
              (gen/limit 10)
              (gen/filter (comp even? :value))
              perfect
              (map :value)))))

(deftest ^:logging log-test
  (is (->> (gen/phases (gen/log :first)
                       (gen/once {:f :a})
                       (gen/log :second)
                       (gen/once {:f :b}))
           perfect
           (map :f)
           (= [:a :b]))))

(deftest mix-test
  (let [fs (->> (gen/mix [(gen/limit 5  {:f :a})
                          (gen/limit 10 {:f :b})])
                perfect
                (map :f))]
    (is (= {:a 5
            :b 10}
           (frequencies fs)))
    (is (not= (concat (repeat 5 :a) (repeat 5 :b)) fs))))

(deftest process-limit-test
  (is (= [[0 0]
          [1 1]
          [3 2]
          [2 3]
          [4 4]]
         (->> (range)
              (map (fn [x] {:value x}))
              (map gen/once)
              (gen/process-limit 5)
              gen/clients
              perfect-info
              (map (juxt :process :value))))))

(defn integers
  "A sequence of maps with :value 0, 1, 2, ..., and any other kv pairs."
  [& kv-pairs]
  (->> (range)
       (map (fn [x] (apply hash-map :value x kv-pairs)))
       (map gen/once)))

(deftest reserve-test
  (let [as (integers :f :a)
        bs (integers :f :b)
        cs (integers :f :c)]
    (testing "only a default"
      (is (= [{:f :a, :process 0, :time 0, :type :invoke, :value 0}
              {:f :a, :process 1, :time 0, :type :invoke, :value 1}
              {:f :a, :process :nemesis, :time 0, :type :invoke, :value 2}]
             (->> (gen/reserve as)
                  (gen/limit 3)
                  perfect))))

    (testing "three ranges"
      (is (= [{:f :c, :process :nemesis, :time 0, :type :invoke, :value 0}
              {:f :c, :process 5, :time 0, :type :invoke, :value 1}
              {:f :a, :process 0, :time 0, :type :invoke, :value 0}
              {:f :a, :process 1, :time 0, :type :invoke, :value 1}
              {:f :b, :process 4, :time 0, :type :invoke, :value 0}
              {:f :b, :process 3, :time 0, :type :invoke, :value 1}
              {:f :b, :process 2, :time 0, :type :invoke, :value 2}
              {:f :b, :process 2, :time 10, :type :invoke, :value 3}
              {:f :b, :process 3, :time 10, :type :invoke, :value 4}
              {:f :b, :process 4, :time 10, :type :invoke, :value 5}
              {:f :a, :process 1, :time 10, :type :invoke, :value 2}
              {:f :a, :process 0, :time 10, :type :invoke, :value 3}
              {:f :c, :process 5, :time 10, :type :invoke, :value 2}
              {:f :c, :process :nemesis, :time 10, :type :invoke, :value 3}
              {:f :c, :process :nemesis, :time 20, :type :invoke, :value 4}]
             (->> (gen/reserve 2 as
                               3 bs
                               cs)
                  (gen/limit 15)
                  (perfect (n+nemesis-context 6))))))))
