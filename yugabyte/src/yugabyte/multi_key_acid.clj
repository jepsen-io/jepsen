(ns yugabyte.multi-key-acid
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [independent :as independent]]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model]
            [clojurewerkz.cassaforte [client :as cassandra]
                                     [query :as q :refer :all]
                                     [cql :as cql]]
            [yugabyte [client :as c]])
  (:import (knossos.model Model)))

(def table-name "multi_key_acid")
(def keyspace "jepsen")

(defrecord MultiRegister []
  Model
  (step [this op]
        (assert (= (:f op) :txn))
        ; `knossos.model.memo/memo` function passing operation invocations as
        ; steps to the model, so in our case we can receive operation which
        ; value is simply `:read` without any sequence and need to handle that
        ; properly by returning current state.
        ;
        ; TODO: this feels... deeply illegal. Figure out why this seems to work
        ; OK anyway?
        (if (= (:value op) :read)
          this
          (reduce
            (fn [state [f k v]]
              ; Apply this particular op
              (case f
                :read  (if (or (nil? v)
                               (= v (get state k)))
                         state
                         (reduced
                           (model/inconsistent
                             (str k ": " (pr-str (get state k)) "≠" (pr-str v)))))
                :write (assoc state k v)))
            this
            (:value op)))))

(defn multi-register
  "A register supporting read and write transactions over registers identified
  by keys. Takes a map of initial keys to values. Supports a single :f for ops,
  :txn, whose value is a transaction: a sequence of [f k v] tuples, where :f is
  :read or :write, k is a key, and v is a value. Nil reads are always legal."
  [values]
  (map->MultiRegister values))

(c/defclient CQLMultiKey keyspace []
  (setup! [this test]
    (c/create-transactional-table
      conn table-name
      (q/if-not-exists)
      (q/column-definitions {:id          :int
                             :ik          :int
                             :val         :int
                             :primary-key [:id :ik]})))

  (invoke! [this test op]
    (assert (= (:f op) :txn))
    (let [[ik value] (:value op)]
      (if (= value :read)
        (assoc op :type :ok :value
               (->> (cql/select-with-ks conn keyspace table-name
                                        (where [[= :ik ik ]]))
                    (map (fn [x] [:read (:id x) (:val x)]))
                    (sort-by second)
                    (independent/tuple ik)))

        ; TODO - temporarily replaced by single DB call until YugaByteDB supports transaction spanning
        ; multiple DB calls.
        ;                 (cassandra/execute conn "BEGIN TRANSACTION")
        ;                 (doseq [[sub-op id val] value]
        ;                   (assert (= sub-op :write))
        ;                   (cql/insert conn table-name {:id id :val val}))
        ;                 (cassandra/execute conn "END TRANSACTION;")
        (do (cassandra/execute conn
                               (str "BEGIN TRANSACTION "
                                    (->> (for [[sub-op id val] value]
                                           (do
                                             (assert (= sub-op :write))
                                             (str "INSERT INTO "
                                                  keyspace "." table-name
                                                  " (id, ik, val) VALUES ("
                                                  id ", " ik ", " val ");")))
                                         clojure.string/join)
                                    "END TRANSACTION;"))
            (assoc op :type :ok)))))

  (teardown! [this test]))

(defn r [_ _] {:type :invoke, :f :txn :value :read})

(defn w [_ _]
  {:type  :invoke,
   :f     :txn
   :value [[:write (rand-int 3) (rand-int 5)]
           [:write (rand-int 3) (rand-int 5)]]})

(defn workload
  [opts]
  (let [n (count (:nodes opts))]
    {:client           (->CQLMultiKey)
     :generator (independent/concurrent-generator
                  (* 2 n)
                  (range)
                  (fn [k]
                    (->> (gen/reserve n r w)
                         (gen/stagger 0.1)
                         (gen/limit 100))))
     :checker (independent/checker
                (checker/compose
                  {:timeline (timeline/html)
                   :linear   (checker/linearizable
                               {:model (multi-register {})})}))}))
