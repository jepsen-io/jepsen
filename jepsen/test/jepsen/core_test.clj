(ns jepsen.core-test
  (:refer-clojure :exclude [run!])
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]
                     [test :refer :all]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [common-test :refer [quiet-logging]]
                    [control :as control]
                    [core :refer :all]
                    [db :as db]
                    [generator :as gen]
                    [history :as h]
                    [nemesis :as nemesis]
                    [nemesis-test :as nemesis-test]
                    [os :as os]
                    [store :as store]
                    [tests :as tst]
                    [util :as util]]
            [jepsen.generator.context :as gen.ctx]
            [jepsen.tests.cycle.append :as list-append])
  (:import (jepsen.history IHistory
                           Op)))

(use-fixtures :once quiet-logging)

(defn tracking-client
  "Tracks connections in an atom."
  ([conns]
   (tracking-client conns (atom 0)))
  ([conns uid]
   (reify client/Client
     (open! [c test node]
       (let [uid (swap! uid inc)] ; silly hack
         (swap! conns conj uid)
         (tracking-client conns uid)))

     (setup! [c test] c)

     (invoke! [c test op]
       (assoc op :type :ok))

     (teardown! [c test] c)

     (close! [c test]
       (swap! conns disj uid)))))

(deftest most-interesting-exception-test
  ; Verifies that we get interesting, rather than interrupted or broken barrier
  ; exceptions, out of tests
  (let [db (reify db/DB
             (setup! [this test node]
               ; One thread throws
               (when (= (nth (:nodes test) 2) node)
                      (throw (RuntimeException. "hi")))

               (throw (java.util.concurrent.BrokenBarrierException. "oops")))

             (teardown! [this test node]))
        test (assoc tst/noop-test
                    :pure-generators true
                    :name   "interesting exception"
                    :db     db
                    :ssh    {:dummy? true})]
    (is (thrown-with-msg? RuntimeException #"^hi$" (run! test)))))

(defn list-append-test
  "Tests a list-append workload on a simple in-memory database. Runs n ops.
  Helpful stress & sanity test for generators, writing and reading histories,
  and the Elle checker."
  [n]
  (let [state (atom {})
        ; Takes a state, a txn, and a volatile for the completed txn to go to.
        ; Applies txn to state, returning new state, and updating volatile.
        apply-txn (fn apply-txn [state txn txn'-volatile]
                    (loopr [state' (transient state)
                            txn'  (transient [])]
                           [[f k v :as mop] txn]
                           (case f
                             :r (recur state'
                                       (conj! txn' [f k (get state' k)]))
                             :append (recur (assoc! state' k
                                                    (conj (get state' k []) v))
                                            (conj! txn' mop)))
                           (do (vreset! txn'-volatile (persistent! txn'))
                               (persistent! state'))))
        t1 (volatile! nil)
        client (reify client/Client
                 (open! [this test node] this)
                 (setup! [this test] this)
                 (invoke! [this test op]
                   (let [txn' (volatile! nil)]
                     (swap! state apply-txn (:value op) txn')
                     (assoc op :type :ok, :value @txn')))
                 (teardown! [this test]
                   (vreset! t1 (System/nanoTime)))
                 (close! [this test]))
        test  (-> tst/noop-test
                  (merge (list-append/test {})
                         {:name "list-append"
                          :client client
                          :concurrency 100
                          :ssh {:dummy? true}})
                  (update :generator #(->> %
                                           gen/clients
                                           (gen/limit n))))
        t0 (System/nanoTime)
        test (run! test)
        t1 @t1
        t2 (System/nanoTime)
        h (:history test)
        r (:results test)]
    (testing "history"
      (is (= (* 2 n) (count h)))
      (is (instance? IHistory h))
      (is (instance? Op (first h))))
    (testing "results"
      (is (= true (:valid? r))))
    (assoc test
           :run-time   (double (util/nanos->secs (- t1 t0)))
           :check-time (double (util/nanos->secs (- t2 t1))))))

(deftest list-append-short-test
  (list-append-test 100))

(deftest ^:perf list-append-perf-test
  (let [n (long 1e6)
        {:keys [run-time check-time]} (list-append-test n)]
    (println (format "list-append-perf-test: %d ops run in %.2f s (%.2f ops/sec); checked in %.2f s (%.2f ops/sec)"
                     n run-time (/ n run-time)
                     check-time (/ n check-time)))))

(deftest ^:integration basic-cas-test
  (let [state (atom nil)
        meta-log (atom [])
        db    (tst/atom-db state)
        n     1000
        nemesis (nemesis-test/test-nem :nem #{:fault})
        test (assoc tst/noop-test
                       :name      "basic cas pure-gen"
                       :db        db
                       :client    (tst/atom-client state meta-log)
                       :nemesis   nemesis
                       :concurrency 10
                       :pure-generators true
                       :generator
                       (->> (gen/phases
                              {:f :read}
                              (->> (gen/reserve
                                     5 (repeat {:f :read})
                                     (gen/mix
                                       [(fn [] {:f :write
                                                :value (rand-int 5)})
                                        (fn [] {:f :cas
                                                :value [(rand-int 5)
                                                        (rand-int 5)]})]))
                                   (gen/limit n)))
                            (gen/nemesis {:type :info, :f :fault})))
        test     (run! test)
        h        (:history test)
        invokes  (partial filter h/invoke?)
        oks      (partial filter h/ok?)
        reads    (partial filter (comp #{:read} :f))
        writes   (partial filter (comp #{:write} :f))
        cases    (partial filter (comp #{:cas} :f))
        values   (partial map :value)
        smol?    #(<= 0 % 4)
        smol-vec? #(and (vector? %)
                        (= 2 (count %))
                        (every? smol? %))]
    (testing "db teardown"
      (is (= :done @state)))

    (testing "client setup/teardown"
      (let [n         (count (:nodes test))
            n2        (* 2 n)
            setup     (take n2 @meta-log)
            run       (->> @meta-log (drop n2) (drop-last n2))
            teardown  (take-last n2 @meta-log)]
        (is (= {:open     n   :setup n}   (frequencies setup)))
        (is (= {:open     n2  :close n2}  (frequencies run)))
        (is (= {:teardown n   :close n}   (frequencies teardown)))))

    (testing "nemesis setup/teardown"
      (let [ops (filter (comp #{:nemesis} :process) h)
            _   (is (= 2 (count ops)))
            [op op'] ops]
        ; Generator should have produced a fault
        (is (= :fault (:f op)))
        (is (= :fault (:f op')))
        ; The nemesis should have been set up, but we won't have seen that
        ; change the original nemesis.
        (is (false? (:setup? nemesis)))
        ; But since we write the nemesis to the completion value, THAT should
        ; have been set up.
        (let [nemesis' (:value op')]
          (is (= :nem (:id nemesis')))
          (is (true? (:setup? nemesis'))))
        ; Finally, we want to make sure we actually tore down the nemesis. This
        ; one's stateful, so we read it from the original nemesis.
        (is (true? @(:teardown? nemesis)))))

    (is (:valid? (:results test)))
    (testing "first read"
      (is (= 0 (:value (first (oks (reads h)))))))
    (testing "history"
      ; 1 initial read, 1 nemesis fault
      (is (= (* 2 (+ 2 n)) (count h)))
      (is (= #{:read :write :cas :fault} (set (map :f h))))
      (is (every? nil? (values (invokes (reads h)))))
      (is (every? smol? (values (oks (reads h)))))
      (is (every? smol? (values (writes h))))
      (is (every? smol-vec? (values (cases h)))))))

(deftest ^:integration ssh-test
  (let [os-startups  (atom {})
        os-teardowns (atom {})
        db-startups  (atom {})
        db-teardowns (atom {})
        db-primaries (atom [])
        nonce        (rand-int Integer/MAX_VALUE)
        nonce-file   "/tmp/jepsen-test"
        test (run! (assoc tst/noop-test
                          :name      "ssh test"
                          :pure-generators true
                          :os (reify os/OS
                                (setup! [_ test node]
                                  (swap! os-startups assoc node
                                         (control/exec :hostname)))

                                (teardown! [_ test node]
                                  (swap! os-teardowns assoc node
                                         (control/exec :hostname))))

                          :db (reify db/DB
                                (setup! [_ test node]
                                  (swap! db-startups assoc node
                                         (control/exec :hostname))
                                  (control/exec :echo nonce :> nonce-file))

                                (teardown! [_ test node]
                                  (swap! db-teardowns assoc node
                                         (control/exec :hostname))
                                  (control/exec :rm :-f nonce-file))

                                db/Primary
                                (setup-primary! [_ test node]
                                  (swap! db-primaries conj
                                         (control/exec :hostname)))

                                db/LogFiles
                                (log-files [_ test node]
                                  [nonce-file]))))]

    (is (:valid? (:results test)))
    (is (apply =
               (str nonce)
               (->> test
                    :nodes
                    (map #(->> (store/path test (name %)
                                           (str/replace nonce-file #".+/" ""))
                               slurp
                               str/trim)))))
    (is (= @os-startups @os-teardowns @db-startups @db-teardowns
           {"n1" "n1"
            "n2" "n2"
            "n3" "n3"
            "n4" "n4"
            "n5" "n5"}))
    (is (= @db-primaries ["n1"]))))

(deftest ^:integration worker-recovery-test
  ; Workers should only consume n ops even when failing.
  (let [invocations (atom 0)
        n 12]
    (run! (assoc tst/noop-test
                 :name "worker recovery"
                 :client (reify client/Client
                           (open!  [c t n] c)
                           (setup! [c t])
                           (invoke! [_ _ _]
                             (swap! invocations inc)
                             (/ 1 0))
                           (teardown! [c t])
                           (close! [c t]))
                 :checker  (checker/unbridled-optimism)
                 :pure-generators true
                 :generator (->> (repeat {:f :read})
                                 (gen/limit n)
                                 (gen/nemesis nil))))
      (is (= n @invocations))))

(deftest ^:integration generator-recovery-test
  ; Throwing an exception from a generator shouldn't break the core. We use
  ; gen/phases to force a synchronization barrier in the generator, which would
  ; ordinarily deadlock when one worker thread prematurely exits, and prove
  ; that we can knock other worker threads out of that barrier and have them
  ; abort cleanly.
  (let [conns (atom #{})]
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Divide by zero"
          (run! (assoc tst/noop-test
                       :name "generator recovery"
                       :client (tracking-client conns)
                       :pure-generators true
                       :generator (gen/clients
                                    (gen/phases
                                      (gen/each-thread
                                        (gen/once
                                          (fn [test ctx]
                                            (if (= [0] (seq (gen.ctx/free-threads ctx)))
                                              (/ 1 0)
                                              {:type :invoke, :f :meow}))))
                                      (gen/once {:type :invoke, :f :done})))))))
    (is (empty? @conns))))

(deftest ^:integration worker-error-test
  ; Errors in client and nemesis setup and teardown should be rethrown from
  ; tests.
  (let [client (fn [t]
                 (reify client/Client
                   (open!     [c test node] (if (= :open t)  (assert false) c))
                   (setup!    [c test]      (if (= :setup t) (assert false)))
                   (invoke!   [c test op]   (assoc op :type :ok))
                   (teardown! [c test]      (if (= :teardown t) (assert false)))
                   (close!    [c test]      (if (= :close t) (assert false)))))
        nemesis (fn [t]
                  (reify nemesis/Nemesis
                    (setup! [n test]        (if (= :setup t) (assert false) n))
                    (invoke! [n test op]    op)
                    (teardown! [n test]     (if (= :teardown t) (assert false)))))
        test (fn [client-type nemesis-type]
               (run! (assoc tst/noop-test
                            :pure-generators true
                            :client   (client client-type)
                            :nemesis  (nemesis nemesis-type))))]
    (testing "client open"      (is (thrown-with-msg? AssertionError #"false" (test :open  nil))))
    (testing "client setup"     (is (thrown-with-msg? AssertionError #"false" (test :setup nil))))
    (testing "client teardown"  (is (thrown-with-msg? AssertionError #"false" (test :teardown nil))))
    (testing "client close"     (is (thrown-with-msg? AssertionError #"false" (test :close nil))))
    (testing "nemesis setup"    (is (thrown-with-msg? AssertionError #"false" (test :setup nil))))
    (testing "nemesis teardown" (is (thrown-with-msg? AssertionError #"false" (test :teardown nil))))))
