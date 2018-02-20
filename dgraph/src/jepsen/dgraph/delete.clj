(ns jepsen.dgraph.delete
  "Tries creating an indexed record, then deleting that record, and checking to
  make sure that indexes are always up to date."
  (:require [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [disorderly with-retry]]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
            [jepsen.tests.bank :as bank])
  (:import (io.dgraph TxnConflictException)))

; We're playing super fast and loose here. Reads, upserts, and deletes are all
; by key. Upserts and deletes just upsert and delete {:key whatever}. Reads
; take a key as their :value, and return :value [key [r1 r2 ...]] where each r
; is a record that matched a query for that particular key.

(defn r [_ _] {:type :invoke, :f :read,   :value (rand-int 16)})
(defn u [_ _] {:type :invoke, :f :upsert, :value (rand-int 16)})
(defn d [_ _] {:type :invoke, :f :delete, :value (rand-int 16)})

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "key: int @index(int) .\n")))

  (invoke! [this test op]
    (c/with-conflict-as-fail op
      (c/with-txn [t conn]
        (case (:f op)
          :read (->> (c/query t (str "{ q(func: eq(key, $key)) {\n"
                                     "  uid\n"
                                     "  key\n"
                                     "}}")
                              {:key (:value op)})
                     :q
                     (vector (:value op))
                     (assoc op :type :ok, :value))

          :upsert (do (c/upsert! t :key {:key (:value op)})
                      (assoc op :type :ok))

          :delete
          (if-let [uid (-> (c/query t "{ q(func: eq(key, $key)) { uid }}"
                                    {:key (:value op)})
                           :q
                           first
                           :uid)]
            (do (c/delete! t uid)
                (assoc op :type :ok))
            (assoc op :type :fail, :error :not-found))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn checker
  "We want to verify that every read either finds an empty set, or exactly one
  record with both a :uid and a :key."
  []
  (reify checker/Checker
    (check [_ test model history opts]
      (let [reads (->> history
                       (filter (fn [{:keys [f type value]}]
                                 ; We want an OK read
                                 (and (= :ok type)
                                      (= :read f)
                                      ; Which didn't...
                                      (not (let [[k results] value]
                                             ; Have nothing found
                                             (or (= 0 (count results))
                                                 ; Or exactly one record
                                                 (and (= 1 (count results))
                                                      (let [v (first results)]
                                                        ; With UID and key
                                                        (and (= #{:uid :key}
                                                                (set (keys v)))
                                                             ; And correct key
                                                             (= k (:key v))))))))))))]
        {:valid? (empty? reads)
         :bad-reads reads}))))


(defn workload
  "Stuff you need to build a test!"
  [opts]
  {:client      (Client. nil)
   :generator   (->> (gen/mix [r u d]))
   :checker     (checker)})
