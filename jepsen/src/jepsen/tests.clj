(ns jepsen.tests
  "Provide utilities for writing tests using jepsen."
  (:require [jepsen.os :as os]
            [jepsen.db :as db]
            [jepsen.client :as client]
            [jepsen.control :as control]
            [jepsen.nemesis :as nemesis]
            [jepsen.checker :as checker]
            [jepsen.net :as net]))

(def noop-test
  "Boring test stub.
  Typically used as a basis for writing more complex tests.
  "
  {:nodes     ["n1" "n2" "n3" "n4" "n5"]
   :name      "noop"
   :os        os/noop
   :db        db/noop
   :net       net/iptables
   :remote    control/ssh
   :client    client/noop
   :nemesis   nemesis/noop
   :generator nil
   :checker   (checker/unbridled-optimism)})

(defn atom-db
  "Wraps an atom as a database."
  [state]
  (reify db/DB
    (setup!    [db test node] (reset! state 0))
    (teardown! [db test node] (reset! state :done))))

(defn atom-client
  "A CAS client which uses an atom for state. Should probably move this into
  core-test."
  ([state]
   (atom-client state (atom [])))
  ([state meta-log]
   (reify client/Client
     (open!     [this test node]
       (swap! meta-log conj :open)
       this)
     (setup!    [this test]
       (swap! meta-log conj :setup)
       this)
     (teardown! [this test] (swap! meta-log conj :teardown))
     (close!    [this test] (swap! meta-log conj :close))
     (invoke!   [this test op]
       ; We sleep here to make sure we actually have some concurrency.
       (Thread/sleep 1)
       (case (:f op)
         :write (do (reset! state   (:value op))
                    (assoc op :type :ok))

         :cas   (let [[cur new] (:value op)]
                  (try
                    (swap! state (fn [v]
                                   (if (= v cur)
                                     new
                                     (throw (RuntimeException. "CAS failed")))))
                    (assoc op :type :ok)
                    (catch RuntimeException e
                      (assoc op :type :fail))))

         :read  (assoc op :type :ok
                       :value @state))))))
