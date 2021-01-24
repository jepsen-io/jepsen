(ns jecci.interface.client
  (:require [jecci.interface.resolver :as r]
            [jepsen.client :as client]
            [clojure.tools.logging :refer [info warn error]]))

(defrecord fake-unresolvable-client [t]
  client/Client
  (open! [this test node]
    (throw (Exception. (str "cannot resolve " t ", not able to run the test"))))
  (setup! [this test]
    (throw (Exception. (str "cannot resolve " t ", not able to run the test"))))
  (invoke! [this test op]
    (throw (Exception. (str "cannot resolve " t ", not able to run the test"))))
  (teardown! [this test]
    (throw (Exception. (str "cannot resolve " t ", not able to run the test"))))
  (close! [this test]
    (throw (Exception. (str "cannot resolve " t ", not able to run the test")))))

(defmacro unresolvable [t]
  `(fn [& ~'args] (fake-unresolvable-client. ~t)))

(def gen-BankClient (if-let [f (r/resolve! "bank" "gen-BankClient")] f
                            (unresolvable "gen-BankClient")))

(def gen-MultiBankClient (if-let [f (r/resolve! "bank" "gen-MultiBankClient")] f
                                 (unresolvable "gen-MultiBankClient")))

(def gen-IncrementClient (if-let [f (r/resolve! "monotonic" "gen-IncrementClient")] f
                                 (unresolvable "gen-IncrementClient")))

(def gen-AtomicClient (if-let [f (r/resolve! "register" "gen-AtomicClient")] f
                              (unresolvable "gen-AtomicClient")))

(def gen-SequentialClient (if-let [f (r/resolve! "sequential" "gen-SequentialClient")] f
                                  (unresolvable "gen-SequentialClient")))

(def gen-SetClient (if-let [f (r/resolve! "sets" "gen-SetClient")] f
                           (unresolvable "gen-SetClient")))

(def gen-CasSetClient (if-let [f (r/resolve! "sets" "gen-CasSetClient")] f
                              (unresolvable "gen-CasSetClient")))

(def gen-TableClient (if-let [f (r/resolve! "table" "gen-TableClient")] f
                             (unresolvable "gen-TableClient")))

(def gen-Client (if-let [f (r/resolve! "txn" "gen-Client")] f
                        (unresolvable "gen-Client")))