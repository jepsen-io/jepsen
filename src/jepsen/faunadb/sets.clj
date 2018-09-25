(ns jepsen.faunadb.sets
  "Set test"
  (:refer-clojure :exclude [test])
  (:use jepsen.faunadb.query)
  (:import com.faunadb.client.types.Codec)
  (:import com.faunadb.client.types.Field)
  (:require [clojure.tools.logging :refer :all]
            [jepsen [client :as client]
                    [checker :as checker]
                    [fauna :as fauna]
                    [generator :as gen]]
            [jepsen.faunadb.client :as f]))

(def classRef
  "Sets class ref"
  (ClassRef (v "jsets")))

(def idxRef
  "All sets index ref"
  (IndexRef (v "all_jsets")))

(def valuePath
  "Path to value data"
  (Arr (v "data") (v "value")))

(def ValuesField
  "A field extractor for values"
  (let [c (Field/as Codec/LONG)
        f (Field/at (into-array String ["data"]))]
    (. f (collect c))))

(defrecord SetsClient [tbl-created? conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (f/query conn (CreateClass (Obj "name" (v "jsets"))))
        (f/query
          conn
          (CreateIndex
            (Obj
              "name" (v "all_jsets")
              "source" classRef
              "values" (Arr (Obj "field" valuePath))))))))

  (invoke! [this test op]
    (case (:f op)
      :add
      (let [setVal (:value op)]
        (f/query
          conn
          (Create
            (Ref classRef (v setVal))
            (Obj "data" (Obj "value" (v setVal)))))
         (assoc op :type :ok))

      :read
      (->>
        ; TODO: What's going on here? What's queryGetAll? What's with the
        ; conj/flatten?
        (f/queryGetAll conn (Match idxRef) ValuesField)
        (mapv (fn [n] (reduce conj [] n)))
        (flatten)
        (set)
        (assoc op :type :ok, :value))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn test
  [opts]
  (fauna/basic-test
    (merge
      {:name   "set"
       :client {:client (SetsClient. (atom false) nil)
                :during (->> (range)
                          (map (partial array-map
                                        :type :invoke
                                        :f :add
                                        :value))
                          gen/seq
                          (gen/stagger 1))
                :final (gen/once {:type :invoke, :f :read, :value nil})}
       :checker (checker/compose
                  {:perf     (checker/perf)
                   :details  (checker/set)})}
      opts)))
