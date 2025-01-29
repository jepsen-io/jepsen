(ns jepsen.faunadb.internal
  "Explores internal consistency of FaunaDB transactions"
  (:refer-clojure :exclude [test])
  (:import com.faunadb.client.errors.UnavailableException)
  (:import java.io.IOException)
  (:import java.util.concurrent.ExecutionException)
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [core :as jepsen]
                    [util :as util :refer [map-keys]]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.faunadb [client :as f]
                            [query :as q]]
            [dom-top.core :as dt]
            [clojure.core.reducers :as r]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as cstr]
            [clojure.tools.logging :refer :all]
            [knossos.op :as op]))

(def cats-name "cats")
(def cats (q/class cats-name))

(def index-name "cats_by_type")
(def index (q/index index-name))

(defn create
  "A query which creates a cat with the given data."
  [data]
  (q/create cats {:data data}))

(defn match
  "A query which returns the first 1024 cats names of the given type."
  [type]
  (as-> (q/match index type) v
    (q/paginate v {:size 1024})
    (q/select ["data"] v)
    (q/map v (q/fn [ref name] name))))

(defn delete-by-type
  "A query which deletes the first 1024 cats of the given type."
  [type]
  (as-> (q/match index type) v
    (q/paginate v {:size 1024})
    (q/select ["data"] v)
    (q/map v (q/fn [ref name] ref))
    (q/foreach v (q/fn [r]
                   ; Indices aren't necessarily serializable, so we have to
                   ; check here to make sure the things we're deleting still
                   ; exist
                   (q/when (q/exists? r)
                     (q/delete r))))))

(defrecord InternalClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (f/client node)))

  (setup! [this test]
    (f/with-retry
      (f/upsert-class! conn {:name cats-name})
      (f/upsert-index! conn {:name index-name
                             :source cats
                             :active true
                             :serialized (boolean (:serialized-indices test))
                             :terms [{:field ["data" "type"]}]
                             :values [{:field ["ref"]}
                                      {:field ["data" "name"]}]})))

  (invoke! [this test op]
    (let [v (:value op)]
      (f/with-errors op #{}
        (let [r (case (:f op)
                  :reset
                  (f/query conn
                           (q/do (delete-by-type "tabby")
                                 (delete-by-type "calico")))

                  :create-tabby-let
                  (f/query conn
                           ; This is neat: temporal queries which happen to be
                           ; at the current txn timestamp observe mutability.
                           (q/let [t (q/time "now")]
                             (q/let [tabbies-0 (q/at t (match "tabby"))
                                     tabby     (create {:type "tabby"
                                                        :name (:value op)})
                                     tabbies-1 (q/at t (match "tabby"))]
                               ; Note that we permute this object literal in the
                               ; opposite order to the let binding, so that we
                               ; ensure we're checking let correctness, not
                               ; object literals.
                               {:t t
                                :tabbies-1 tabbies-1
                                :tabby tabby
                                :tabbies-0 tabbies-0})))

                  :create-tabby-obj
                  (map-keys {:c :tabbies-0
                             :a :tabby
                             :b :tabbies-1}
                            (f/query conn
                                     ; This relies on the fact that short map
                                     ; literals in Clojure use an array map
                                     ; under the hood, which is traversed in
                                     ; the same order as the literal is
                                     ; written, meaning we construct a
                                     ; fauna.lang.Obj with the same order as
                                     ; this code.
                                     {:c (match "tabby")
                                      :a (create {:type "tabby"
                                                  :name (:value op)})
                                      :b (match "tabby")}))

                  :create-tabby-arr
                  (zipmap [:tabbies-0, :tabby, :tabbies-1]
                          (f/query conn
                                   [(match "tabby")
                                    (create {:type "tabby"
                                             :name v})
                                    (match "tabby")]))

                  :change-type
                  (f/query conn [(q/let [rs (as-> (q/match index "tabby") v
                                              (q/paginate v {:size 1})
                                              (q/select ["data"] v)
                                              (q/map v (q/fn [r name] r)))]
                                   (q/when (q/non-empty? rs)
                                     (q/update (q/select [0] rs)
                                               {:data {:type "calico"}})))
                                 (match "tabby")
                                 (match "calico")]))]
          (assoc op :type :ok, :value r)))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn op-errors
  "Returns an list of errors in an operation."
  [op]
  (remove nil?
          (let [v (:value op)]
            (case (:f op)
              :reset nil

              :create-tabby-let
              (let [name (-> v :tabby :data :name)]
                [(when (some #{name} (:tabbies-0 v))
                   {:type :present-before-create
                    :name name
                    :op   op})
                 (when-not (some #{name} (:tabbies-1 v))
                   {:type :missing-after-create
                    :name name
                    :op   op})])

              :create-tabby-obj
              (let [name (-> v :tabby :data :name)]
                [(when (some #{name} (:tabbies-0 v))
                   {:type :present-before-create
                    :name name
                    :op   op})
                 (when-not (some #{name} (:tabbies-1 v))
                   {:type :missing-after-create
                    :name name
                    :op   op})])

              :create-tabby-arr
              (let [name (-> v :tabby :data :name)]
                [(when (some #{name} (:tabbies-0 v))
                   {:type :present-before-create
                    :name name
                    :op   op})
                 (when-not (some #{name} (:tabbies-1 v))
                   {:type :missing-after-create
                    :name name
                    :op   op})])

              :change-type
              (let [[cat tabbies calicos] v]
                (when-let [name (-> cat :data :name)]
                  [(when (some #{name} tabbies)
                     {:type :present-after-change
                      :name name
                      :op   op})
                   (when-not (some #{name} calicos)
                     {:type :missing-after-change
                      :name name
                      :op   op})]))))))

(defn checker
  "Checks internal consistency of ops."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [errors (->> history
                        (r/filter op/ok?)
                        (r/mapcat op-errors)
                        (r/filter seq)
                        (into []))]
        {:valid? (empty? errors)
         :error-count (count errors)
         :error-types (->> errors (r/map :type) (into (sorted-set)))
         :errors errors}))))

(defn gen
  "Generator of ops"
  []
  (let [id (atom -1)
        create-tabby-let (fn [_ _]
                           {:type   :invoke
                            :f      :create-tabby-let
                            :value  (swap! id inc)})
        create-tabby-arr (fn [_ _]
                           {:type   :invoke
                            :f      :create-tabby-arr
                            :value  (swap! id inc)})
        create-tabby-obj (fn [_ _]
                           {:type   :invoke
                            :f      :create-tabby-obj
                            :value  (swap! id inc)})]
    (gen/mix [{:type :invoke, :f :reset, :value nil}
              {:type :invoke, :f :change-type, :value nil}
              create-tabby-let
              create-tabby-arr
              create-tabby-obj])))

(defn workload
  [opts]
  {:client    (InternalClient. nil)
   :generator (gen/stagger 1/10 (gen))
   :checker (checker)})
