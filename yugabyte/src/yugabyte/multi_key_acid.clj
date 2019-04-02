(ns yugabyte.multi-key-acid
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen [client       :as client]
                    [checker      :as checker]
                    [generator    :as gen]
                    [independent  :as independent]
                    [util         :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.txn.micro-op :as mop]
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
    (reduce (fn [state [f k v]]
              ; Apply this particular op
              (case f
                :r (if (or (nil? v)
                           (= v (get state k)))
                     state
                     (reduced
                       (model/inconsistent
                         (str (pr-str (get state k)) "≠" (pr-str v)))))
                :w (assoc state k v)))
            this
            (:value op))))

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
    (c/with-errors op #{:read}
      (let [[ik txn] (:value op)]
        (case (:f op)
          :read
          (let [ks (map mop/key txn)
                ; Look up values
                vs (->> (cql/select conn table-name
                                    (q/columns :id :val)
                                    (q/where [[= :ik ik]
                                              [:in :id ks]]))
                        (map (juxt :id :val))
                        (into {}))
                ; Rewrite txn to use those values
                txn' (mapv (fn [[f k _]] [f k (get vs k)]) txn)]
            (assoc op :type :ok, :value (independent/tuple ik txn')))

          :write
          ; TODO - temporarily replaced by single DB call until YugaByteDB
          ; supports transaction spanning multiple DB calls.
          ;                 (cassandra/execute conn "BEGIN TRANSACTION")
          ;                 (doseq [[sub-op id val] value]
          ;                   (assert (= sub-op :write))
          ;                   (cql/insert conn table-name {:id id :val val}))
          ;                 (cassandra/execute conn "END TRANSACTION;")
          (do (cassandra/execute conn
                                 (str "BEGIN TRANSACTION "
                                      (->> (for [[f k v] txn]
                                             (do
                                               ; We only support writes
                                               (assert (= :w f))
                                               (str "INSERT INTO "
                                                    keyspace "." table-name
                                                    " (id, ik, val) VALUES ("
                                                    k ", " ik ", " v ");")))
                                           clojure.string/join)
                                      "END TRANSACTION;"))
              (assoc op :type :ok))))))

  (teardown! [this test]))

; Three keys, five possible values per key.
(def key-range (vec (range 3)))
(defn rand-val [] (rand-int 5))

(defn r
  "Read a random subset of keys."
  [_ _]
  (->> (util/random-nonempty-subset key-range)
       (mapv (fn [k] [:r k nil]))
       (array-map :type :invoke, :f :read, :value)))

(defn w [_ _]
  "Write a random subset of keys."
  (->> (util/random-nonempty-subset key-range)
       (mapv (fn [k] [:w k (rand-val)]))
       (array-map :type :invoke, :f :write, :value)))

(defn workload
  [opts]
  (let [n (count (:nodes opts))]
    {:client           (->CQLMultiKey)
     :generator (independent/concurrent-generator
                  (* 2 n)
                  (range)
                  (fn [k]
                    (->> (gen/reserve n r w)
                         (gen/stagger 1)
                         (gen/process-limit 20))))
     :checker (independent/checker
                (checker/compose
                  {:timeline (timeline/html)
                   :linear   (checker/linearizable
                               {:model (multi-register {})})}))}))
