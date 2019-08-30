(ns jepsen.core-test
  (:refer-clojure :exclude [run!])
  (:use jepsen.core
        clojure.test
        clojure.pprint
        clojure.tools.logging)
  (:require [clojure.string :as str]
            [jepsen [common-test :refer [quiet-logging]]]
            [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.tests :as tst]
            [jepsen.control :as control]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.store :as store]
            [jepsen.checker :as checker]
            [jepsen.nemesis :as nemesis]
            [knossos.model :as model]))

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
                    :name   "interesting exception"
                    :db     db
                    :ssh    {:dummy? true})]
    (is (thrown-with-msg? RuntimeException #"^hi$" (run! test)))))

(deftest ^:integration basic-cas-test
  (let [state (atom nil)
        db    (tst/atom-db state)
        n     10
        test  (run! (assoc tst/noop-test
                           :name       "basic cas"
                           :db         (tst/atom-db state)
                           :client     (tst/atom-client state)
                           :generator  (->> gen/cas
                                            (gen/limit n)
                                            (gen/nemesis gen/void))
                           :model      (model/->CASRegister 0)))]
    (is (:valid? (:results test)))))

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
                 :generator (->> (gen/queue)
                                 (gen/limit n)
                                 (gen/nemesis gen/void))))
      (is (= n @invocations))))

(deftest ^:integration generator-recovery-test
  ; Throwing an exception from a generator shouldn't break the core. We use
  ; gen/phases to force a synchronization barrier in the generator, which would
  ; ordinarily deadlock when one worker thread prematurely exits, and prove
  ; that we can knock other worker threads out of that barrier and have them
  ; abort cleanly.
  (let [conns (atom #{})]
    (is (thrown-with-msg?
          ArithmeticException #"Divide by zero"
          (run! (assoc tst/noop-test
                       :name "generator recovery"
                       :client (tracking-client conns)
                       :generator (gen/clients
                                    (gen/phases
                                      (gen/each
                                        (gen/once
                                          (reify gen/Generator
                                            (op [_ test process]
                                              (if (= process 0)
                                                (/ 1 0)
                                                {:type :invoke, :f :meow})))))
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
                            :client   (client client-type)
                            :nemesis  (nemesis nemesis-type))))]
    (testing "client open"      (is (thrown-with-msg? AssertionError #"false" (test :open  nil))))
    (testing "client setup"     (is (thrown-with-msg? AssertionError #"false" (test :setup nil))))
    (testing "client teardown"  (is (thrown-with-msg? AssertionError #"false" (test :teardown nil))))
    (testing "client close"     (is (thrown-with-msg? AssertionError #"false" (test :close nil))))
    (testing "nemesis setup"    (is (thrown-with-msg? AssertionError #"false" (test :setup nil))))
    (testing "nemesis teardown" (is (thrown-with-msg? AssertionError #"false" (test :teardown nil))))))
