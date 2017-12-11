(ns jepsen.core-test
  (:refer-clojure :exclude [run!])
  (:use jepsen.core
        clojure.test
        clojure.pprint
        clojure.tools.logging)
  (:require [clojure.string :as str]
            [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.tests :as tst]
            [jepsen.control :as control]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.store :as store]
            [jepsen.checker :as checker]
            [knossos.model :as model]))

(deftest basic-cas-test
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

(deftest ssh-test
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
                                  (control/exec :rm nonce-file))

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

(deftest worker-recovery-test
  ; Workers should only consume n ops even when failing.
  (let [invocations (atom 0)
        n 12]
    (run! (assoc tst/noop-test
                 :name "worker recovery"
                 :client (reify client/Client
                           (setup! [c _ _] c)
                           (invoke! [_ _ _]
                             (swap! invocations inc)
                             (/ 1 0))
                           (teardown! [c _]))
                 :checker  (checker/unbridled-optimism)
                 :generator (->> (gen/queue)
                                 (gen/limit n)
                                 (gen/nemesis gen/void))))
      (is (= n @invocations))))

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

(deftest generator-recovery-test
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
