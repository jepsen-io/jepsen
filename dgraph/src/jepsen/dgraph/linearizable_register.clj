(ns jepsen.dgraph.linearizable-register
  "With server-side ordering, Dgraph is supposed to offer linearizability. This
  test verifies that a single UID and predicate supports linearizable read,
  write, and CAS."
  (:refer-clojure :exclude [read])
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [independent :as independent]]
            [jepsen.generator.pure :as gen]
            [jepsen.tests.linearizable-register :as lr]))

(defn read
  "Given a transaction, read the current uid and value of a key, as a map. If
  no record matches, returns nil."
  [t k]
  (let [results (c/query t "{ q(func: eq(key, $key)) { uid, value } }"
                         {:key k})]
    (assert (< (count (:q results)) 2)
            (str "Expected at most one record to match key " k ", but found"
                 (pr-str results)))
    (first (:q results))))

(defn read-info->fail
  "Read timeouts can safely be considered failures, as reads are idempotent.
  Takes a response operation and converts :type :info to :type :fail, if :f is
  :read."
  [op]
  (if (and (= :read (:f op)) (= :info (:type op)))
    (assoc op :type :fail)
    op))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "key:   int @index(int)"
                               (when (:upsert-schema test) " @upsert")
                               " .\n"
                               "value: int .\n")))

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (c/with-unavailable-backoff
        (read-info->fail
          (c/with-conflict-as-fail op
            (c/with-txn [t conn]
              (case (:f op)
                :read (assoc op
                             :type  :ok
                             :value (independent/tuple k (:value (read t k))))

                :write (do (if-let [record (read t k)]
                             (c/mutate! t {:uid (:uid record), :value v})
                             (c/mutate! t {:key k, :value v}))
                           (assoc op :type :ok))

                :cas   (let [[v v'] v
                             record (read t k)]
                         (if (= v (:value record))
                           (do (c/mutate! t (assoc record :value v'))
                               (assoc op :type :ok))
                           (assoc op :type :fail, :error :value-mismatch))))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  (-> (lr/test opts)
      (update :generator (partial gen/stagger 1/100))
      (merge {:client (Client. nil)})))

(defn uid-read
  "Fetch the {:uid uid, :value value} for a key, given a transaction and a UID
  map atom."
  [t uid-map k]
  (when-let [u (get @uid-map k)]
    (let [results (:q (c/query t "{ q(func: uid($u)) { uid, value } }" {:u u}))]
      (assert (< (count results) 2)
              (str "Expected at most one record to match UID " k ", but found"
                   (pr-str results)))
      (first results))))

; UIDs is a map of key to UID; we use this to avoid needing an @upsert schema,
; which I suspect could be introducing false linearization points into our txns
(defrecord UidClient [conn uids]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "value: int .\n")))

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (c/with-unavailable-backoff
        (read-info->fail
          (c/with-conflict-as-fail op
            (c/with-txn [t conn]
              (case (:f op)
                :read (assoc op
                             :type :ok
                             :value (independent/tuple
                                      k (:value (uid-read t uids k))))

                :write (if-let [u (get @uids k)]
                             ; We have a UID for this key already
                             (do (c/mutate! t {:uid u, :value v})
                                 (assoc op :type :ok))

                             ; We've got to insert a new record
                             (let [u (val (first (c/mutate! t {:value v})))
                                   ; Record this as the UID for our key, iff
                                   ; nobody else has created a UID for this key
                                   ; in the meantime
                                   uids (swap! uids (fn [uids]
                                                      (if (get uids k)
                                                        uids
                                                        (assoc uids k u))))]
                               ; If we won the race to insert this value, then
                               ; our write also succeeded. If we didn't win the
                               ; race, then we'll declare this write a failure;
                               ; it won't be read again.
                               (if (= u (get uids k))
                                 (assoc op :type :ok)
                                 (assoc op
                                        :type :fail
                                        :error :lost-uid-race))))

                :cas (let [[v v'] v]
                       (if-let [record (uid-read t uids k)]
                         ; We've got a record!
                         (if (= v (:value record))
                           (do (c/mutate! t (assoc record :value v'))
                               (assoc op :type :ok))
                           (assoc op :type :fail, :error :value-mismatch))
                         ; No record
                         (assoc op :type :fail, :error :not-found))))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn uid-workload
  "A variant of the single-register test which uses UIDs directly, to avoid
  going through indices"
  [opts]
  (-> (lr/test (merge {:per-key-limit 1024} opts))
      (update :generator (partial gen/stagger 1))
      (merge {:client (UidClient. nil (atom {}))})))
