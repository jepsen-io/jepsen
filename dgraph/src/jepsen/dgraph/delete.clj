(ns jepsen.dgraph.delete
  "Tries creating an indexed record, then deleting that record, and checking to
  make sure that indexes are always up to date."
  (:require [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [disorderly with-retry]]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [independent :as independent]]
            [jepsen.generator.pure :as gen]
            [jepsen.checker.timeline :as timeline])
  (:import (io.dgraph TxnConflictException)))

; These operations apply to a single key; we generalize to multiple keys using
; jepsen.independent. Upserts and deletes just upsert and delete {:key
; whatever}. Reads return the set of matching records for the key.

(defn r [_ _] {:type :invoke, :f :read,   :value nil})
(defn u [_ _] {:type :invoke, :f :upsert, :value nil})
(defn d [_ _] {:type :invoke, :f :delete, :value nil})

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "key: int @index(int)"
                               (when (:upsert-schema test) " @upsert")
                               " .\n")))

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (c/with-conflict-as-fail op
        (c/with-txn [t conn]
          (case (:f op)
            :read (->> (c/query t (str "{ q(func: eq(key, $key)) {\n"
                                       "  uid\n"
                                       "  key\n"
                                       "}}")
                                {:key k})
                       :q
                       (independent/tuple k)
                       (assoc op :type :ok, :value))

            :upsert (if-let [uid (c/upsert! t :key {:key k})]
                      (assoc op :type :ok, :uid uid)
                      (assoc op :type :fail, :error :present))

            :delete
            (if-let [uid (-> (c/query t "{ q(func: eq(key, $key)) { uid }}"
                                      {:key k})
                             :q
                             first
                             :uid)]
              (do (c/delete! t uid)
                  (assoc op :type :ok, :uid uid))
              (assoc op :type :fail, :error :not-found)))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn checker
  "We want to verify that every read either finds an empty set, or exactly one
  record with both a :uid and a :key."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [k (:history-key opts)
            reads (->> history
                       (filter (fn [{:keys [f type value]}]
                                 ; We want an OK read
                                 (and (= :ok type)
                                      (= :read f)
                                      ; Which didn't...
                                      (not ; Find nothing
                                           (or (= 0 (count value))
                                               ; Or exactly one record
                                               (and (= 1 (count value))
                                                    (let [v (first value)]
                                                      ; With UID and key
                                                      (and (= #{:uid :key}
                                                              (set (keys v)))
                                                           (= k (:key v)))))))))))]
        {:valid? (empty? reads)
         :bad-reads reads}))))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  {:client      (Client. nil)
   :generator   (independent/pure-concurrent-generator
                  (* 2 (count (:nodes opts)))
                  (range)
                  (fn [k]
                    (->> (gen/mix [r u d])
                         (gen/limit 1000)
                         (gen/stagger 1/10))))
   :checker     (independent/checker
                  (checker/compose
                    {:deletes (checker)
                     :timeline (timeline/html)}))})
