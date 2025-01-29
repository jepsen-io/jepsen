(ns jepsen.stolon.ledger
  "A test which aims to concretely demonstrate the impact of G2-item anomalies
  we found using the list-append test. We store a simulated bank ledger where
  each transaction is a row. Withdrawals require a positive balance across all
  rows. We attempt a double-spend attack, which should fail under
  serializability."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [dom-top.core :refer [with-retry]]
            [jepsen [checker :as checker]
             [client :as client]
             [checker :as checker]
             [generator :as gen]
             [util :as util :refer [parse-long]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.stolon [client :as c]]
            [knossos.op :as op]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]))

(def table "ledger")

(defn add-entry!
  "Adds an account, amount pair to the ledger."
  [conn id account amount]
  (j/execute!
    conn
    [(str "insert into " table " (id, account, amount) values (?, ?, ?)")
     id account amount]))

(defn balance
  "Selects the total of all ledger rows for an account."
  [conn test account]
  (or (:sum (j/execute-one! conn [(str "select sum(amount) from " table
                                       " where account = ?")
                                  account]))
      ; sum of no rows is null, not 0
      0))

(defn balance-select
  "Selects the total of all ledger rows, by doing a direct read and summing
  ourselves."
  [conn test account id]
  (->> (j/execute! conn [(str "select (amount) from " table
                              " where account = ? and id != ?")
                         account id]
                   {:builder-fn rs/as-unqualified-lower-maps})
       (map :amount)
       (reduce + 0)))

(defn transfer!
  "Alters the account balance by inserting a new ledger row iff the total of
  all rows would remain non-negative. Returns true if the transaction
  completed, false otherwise."
  [conn test id account amount]
  (if (pos? amount)
    (add-entry! conn id account amount)
    ; Check for minimum balance
    (let [balance (balance-select conn test account id)]
      (info :balance balance)
      (if (neg? (+ balance amount))
        false
        (do (Thread/sleep (rand-int 10))
            (add-entry! conn id account amount)
            true)))))

; initialized? is an atom which we set when we first use the connection--we set
; up initial isolation levels, logging info, etc. This has to be stateful
; because we don't necessarily know what process is going to use the connection
; at open! time. next-id is an atom we use to choose row identifiers.
(defrecord Client [node conn initialized? next-id]
  client/Client
  (open! [this test node]
    (let [c (c/open test node)]
      (assoc this
             :node          node
             :conn          c
             :initialized?  (atom false))))

  (setup! [_ test]
    ; OK, so first worrying thing: why can this throw duplicate key errors if
    ; it's executed with "if not exists"? Ah, this is a known
    ; non-transactional DDL thing.
    (with-retry [conn  conn
                 tries 10]
      (j/execute! conn
                  [(str "create table if not exists " table
                        " (id      int primary key,
                           account int not null,
                           amount  int not null)")])
      (j/execute! conn
                  [(str "create index i_account on " table " (account)")])
      (catch org.postgresql.util.PSQLException e
          (condp re-find (.getMessage e)
            #"duplicate key value violates unique constraint"
            :dup

            #"An I/O error occurred|connection has been closed"
            (do (when (zero? tries)
                  (throw e))
                (info "Retrying IO error")
                (Thread/sleep 1000)
                (c/close! conn)
                (retry (c/await-open test node)
                       (dec tries)))

            (throw e))))

      ; Make sure we start fresh--in case we're using an existing postgres
      ; cluster and the DB automation isn't wiping the state for us.
      (j/execute! conn [(str "delete from " table)]))

  (invoke! [_ test op]
    ; One-time connection setup
    (when (compare-and-set! initialized? false true)
      (j/execute! conn [(str "set application_name = 'jepsen process "
                        (:process op) "'")])
      (c/set-transaction-isolation! conn (:isolation test)))

    (let [[account amount] (:value op)]
      (c/with-errors op
        (j/with-transaction [t conn
                             {:isolation (:isolation test)}]
          (assoc op :type (if (transfer! conn test
                                         (swap! next-id inc)
                                         account amount)
                            :ok
                            :fail))))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))

(defn check-account
  "Takes an [account, ops] tuple, and checks to make sure it has a non-negative
  balance."
  [[account ops]]
  ; When ops are indeterminate, we don't know if they took place or not. We
  ; have to take the most charitable interpretation: assume deposits succeed,
  ; withdrawals fail.
  (let [deposits    (filter (comp pos? second :value) ops)
        withdrawals (filter (comp neg? second :value) ops)
        balance     (->> (concat deposits (filter op/ok? withdrawals))
                         (map (comp second :value))
                         (reduce + 0))]
    (when (or (neg? balance) (pos? balance))
      {:account account
       :balance balance})))

(defn checker
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [errs (->> history
                      (filter (comp #{:ok :info} :type))
                      (group-by (comp first :value))
                      (keep check-account))]
        {:valid? (not (seq errs))
         :errors errs}))))

(defn fund-then-double-spend-gen
  []
  (->> (range)
       (map (fn [account]
              ; Initial fund
              (cons {:f :transfer, :value [account 10]}
                    ; Double-spend attack
                    (repeat (Math/pow 2 (rand-int 5))
                            {:f :transfer, :value [account -9]}))))))

(defn rand-gen
  []
  (->> (range)
       (map (fn [account]
              (->> (fn gen [] {:f :transfer, :value [account
                                                     (- (rand-int 5) 3)]})
                   (gen/limit 16))))))

(defn workload
  "A package of client, checker, etc."
  [opts]
  {:client    (Client. nil nil nil (atom 0))
   :checker   (checker)
   :generator (rand-gen)})
