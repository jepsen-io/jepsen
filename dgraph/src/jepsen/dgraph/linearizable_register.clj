(ns jepsen.dgraph.linearizable-register
  "With server-side ordering, Dgraph is supposed to offer linearizability. This
  test verifies that a single UID and predicate supports linearizable read,
  write, and CAS."
  (:refer-clojure :exclude [read])
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [generator :as gen]
                    [independent :as independent]]
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
      (c/with-conflict-as-fail op
        (c/with-txn test [t conn]
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
                       (assoc op :type :fail, :error :value-mismatch))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  (-> (lr/test opts)
      (update :generator (partial gen/stagger 1))
      (merge {:client (Client. nil)})))
