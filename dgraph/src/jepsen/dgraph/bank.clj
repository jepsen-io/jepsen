(ns jepsen.dgraph.bank
  "Implements a bank-account test, where we transfer amounts between a pool of
  accounts, and verify that reads always see a constant amount."
  (:require [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [with-retry]]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [generator :as generator]]
            [jepsen.tests.bank :as bank])
  (:import (io.dgraph TxnConflictException)))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (with-retry [attempts 20]
      (c/alter-schema! conn
                       "name: string @index(exact) .")
                       ;"type: string @index(term)")
      (catch io.grpc.StatusRuntimeException e
        (cond (<= attempts 1)
              (throw e)

              (and (.getCause e)
                   (instance? java.net.ConnectException
                              (.getCause (.getCause e))))
              (do (info "GRPC interface unavailable, retrying in 5 seconds")
                  (Thread/sleep 5000)
                  (retry (dec attempts)))

              (re-find #"server is not ready to accept requests"
                       (.getMessage e))
              (do (info "Server not ready, retrying in 5 seconds")
                  (Thread/sleep 5000)
                  (retry (dec attempts)))

              :else
              (throw e))))

    (try
      (c/with-txn [t conn]
        (c/mutate! t {:name "kyle"
                      :type "engineer"}))
      (catch TxnConflictException e))

    (info "Ready.")
    (read-line))

  (invoke! [this test op]
    (info "here")
    (case (:f op)
      :read     op
      :transfer op))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  (merge (bank/test)
         {:client (Client. nil)}))
