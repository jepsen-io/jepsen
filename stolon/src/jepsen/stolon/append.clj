(ns jepsen.stolon.append
  "Test for transactional list append."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [dom-top.core :refer [with-retry]]
            [elle.core :as elle]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]
             [util :as util :refer [parse-long]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.stolon [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]))

(def default-table-count 3)

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn append-using-on-conflict!
  "Appends an element to a key using an INSERT ... ON CONFLICT statement."
  [conn test table k e]
  (j/execute!
    conn
    [(str "insert into " table " as t"
          " (id, sk, val) values (?, ?, ?)"
          " on conflict (id) do update set"
          " val = CONCAT(t.val, ',', ?) where "
          "t.id"
          ;(if (< (rand) 0.5) "t.id" "t.sk")
          " = ?")
     k k e e k]))

(defn insert!
  "Performs an initial insert of a key with initial element e. Catches
  duplicate key exceptions, returning true if succeeded. If the insert fails
  due to a duplicate key, it'll break the rest of the transaction, assuming
  we're in a transaction, so we establish a savepoint before inserting and roll
  back to it on failure."
  [conn test txn? table k e]
  (try
    (info (if txn? "" "not") "in transaction")
    (when txn? (j/execute! conn ["savepoint upsert"]))
    (info :insert (j/execute! conn
                              [(str "insert into " table " (id, sk, val)"
                                    " values (?, ?, ?)")
                               k k e]))
    (when txn? (j/execute! conn ["release savepoint upsert"]))
    true
    (catch org.postgresql.util.PSQLException e
      (if (re-find #"duplicate key value" (.getMessage e))
        (do (info (if txn? "txn") "insert failed: " (.getMessage e))
            (when txn? (j/execute! conn ["rollback to savepoint upsert"]))
            false)
        (throw e)))))

(defn update!
  "Performs an update of a key k, adding element e. Returns true if the update
  succeeded, false otherwise."
  [conn test table k e]
  (let [res (-> conn
                (j/execute-one! [(str "update " table " set val = CONCAT(val, ',', ?)"
                                      " where id = ?") e k]))]
    (info :update res)
    (-> res
        :next.jdbc/update-count
        pos?)))

(defn mop!
  "Executes a transactional micro-op on a connection. Returns the completed
  micro-op."
  [conn test txn? [f k v]]
  (let [table-count (:table-count test default-table-count)
        table (table-for table-count k)]
    (Thread/sleep (rand-int 10))
    [f k (case f
           :r (let [r (j/execute! conn
                                  [(str "select (val) from " table " where "
                                        ;(if (< (rand) 0.5) "id" "sk")
                                        "id"
                                        " = ? ")
                                   k]
                                  {:builder-fn rs/as-unqualified-lower-maps})]
                (when-let [v (:val (first r))]
                  (mapv parse-long (str/split v #","))))

           :append
           (let [vs (str v)]
             (if (:on-conflict test)
               ; Use ON CONFLICT
               (append-using-on-conflict! conn test table k vs)
               ; Try an update, and if that fails, back off to an insert.
               (or (update! conn test table k vs)
                   ; No dice, fall back to an insert
                   (insert! conn test txn? table k vs)
                   ; OK if THAT failed then we probably raced with another
                   ; insert; let's try updating again.
                   (update! conn test table k vs)
                   ; And if THAT failed, all bets are off. This happens even
                   ; under SERIALIZABLE, but I don't think it technically
                   ; VIOLATES serializability.
                   (throw+ {:type     ::homebrew-upsert-failed
                            :key      k
                            :element  v})))
             v))]))

; initialized? is an atom which we set when we first use the connection--we set
; up initial isolation levels, logging info, etc. This has to be stateful
; because we don't necessarily know what process is going to use the connection
; at open! time.
(defrecord Client [node conn initialized?]
  client/Client
  (open! [this test node]
    (let [c (c/open test node)]
      (assoc this
             :node          node
             :conn          c
             :initialized?  (atom false))))

  (setup! [_ test]
    (dotimes [i (:table-count test default-table-count)]
      ; OK, so first worrying thing: why can this throw duplicate key errors if
      ; it's executed with "if not exists"?
      (with-retry [conn  conn
                   tries 10]
        (j/execute! conn
                    [(str "create table if not exists " (table-name i)
                          " (id int not null primary key,
                          sk int not null,
                          val text)")])
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
                (retry (c/await-open node)
                       (dec tries)))

            (throw e))))
      ; Make sure we start fresh--in case we're using an existing postgres
      ; cluster and the DB automation isn't wiping the state for us.
      (j/execute! conn [(str "delete from " (table-name i))])))

  (invoke! [_ test op]
    ; One-time connection setup
    (when (compare-and-set! initialized? false true)
      (j/execute! conn [(str "set application_name = 'jepsen process "
                        (:process op) "'")])
      (c/set-transaction-isolation! conn (:isolation test)))

    (c/with-errors op
      (let [txn       (:value op)
            use-txn?  (< 1 (count txn))
            txn'      (if use-txn?
                      ;(if true
                        (j/with-transaction [t conn
                                             {:isolation (:isolation test)}]
                          (mapv (partial mop! t test true) txn))
                        (mapv (partial mop! conn test false) txn))]
        (assoc op :type :ok, :value txn'))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "A list append workload."
  [opts]
  (-> (append/test (assoc (select-keys opts [:key-count
                                             :max-txn-length
                                             :max-writes-per-key])
                          :min-txn-length 1
                          :consistency-models [(:expected-consistency-model opts)]))
      (assoc :client (Client. nil nil nil))))
