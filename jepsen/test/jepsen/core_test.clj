(ns jepsen.core-test
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
            [jepsen.model :as model]
            [jepsen.checker :as checker]))

(deftest basic-cas-test
  (let [state (atom nil)
        db    (tst/atom-db state)
        n     10
        test  (run! (assoc tst/noop-test
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
        n 30]
    (run! (assoc tst/noop-test
                 :client (reify client/Client
                           (setup! [c _ _] c)
                           (invoke! [_ _ _]
                             (swap! invocations inc)
                             (assert false))
                           (teardown! [c _]))
                 :checker  (checker/unbridled-optimism)
                 :generator (->> (gen/queue)
                                 (gen/limit n)
                                 (gen/nemesis gen/void))))
      (is (= n @invocations))))

(defrecord RecoveryClient [process]
  client/Client
  (setup! [this _ _] (assoc this :process (promise)))
  (teardown! [_ _])
  (open! [_ _ _])
  (close! [_ _])
  (invoke! [this _ op]
    (let [_ (deliver (:process this) (:process op))]
      (condp < (rand)
        0.75 (assoc op :type :info)
        0.50 (assoc op :type :fail)
        0.25 (assoc op :type :ok)
        (throw (Exception. "Please recover, young client"))))))

(deftest client-recovery-test
  ;; Clients should be able to maintain the same process with a failing connection
  (let [n 30
        client (->RecoveryClient nil)
        test (run! (assoc tst/noop-test
                          :client    client
                          :generator  (->> (gen/queue)
                                           (gen/limit n)
                                           (gen/nemesis gen/void))))
        original-process #{@(:process client)}
        processes (set (map :process (:history test)))]
    (is (= original-process processes))))


(deftest open-compat-test []
  (testing "Calls open! when available"
    (let [n 30
          client (reify client/Client
                   (open! [this test client] this)
                   (close! [this test])
                   (invoke! [this test op] (assoc op :type :ok)))
          ;; TODO Run this without a test
          test (run! (assoc tst/noop-test
                            :client client
                            :generator (->> (gen/cas)
                                            (gen/limit n)
                                            (gen/nemesis gen/void))))
          ]
      ))
  (testing "Falls back to setup! when open! is not available"
    (let [])))

(deftest close-compat-test []
  (testing "Calls close! when available"
    (let [n 30
          client (reify client/Client
                   (open! [this test client] this)
                   (close! [this test])
                   (invoke! [this test op] (assoc op :type :ok)))
          ;; TODO Run this in isolation
          test (assoc tst/noop-test
                      :client client
                      :generator (->> (gen/cas)
                                      (gen/limit n)
                                      (gen/nemesis gen/void)))
          ]
      ))
  (testing "Falls back to teardown! when close! is not available"
    (let [])))
