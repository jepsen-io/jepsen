(ns jepsen.dgraph.set
  "Does a bunch of inserts; verifies that all inserted triples are present in a
  final read."
  (:require [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [with-retry]]
            [knossos.op :as op]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "type: string @index(exact)"
                               (when (:upsert-schema test) " @upsert")
                               " .\n"
                               "value: int @index(int) .\n")))

  (invoke! [this test op]
    (c/with-conflict-as-fail op
      (c/with-txn [t conn]
        (case (:f op)
          :add (let [inserted (c/mutate! t {:type "element",
                                            :value (:value op)})]
                 (assoc op :type :ok, :uid (first (vals inserted))))

          :read (->> (c/query t "{ q(func: eq(type, $type)) { uid, value } }"
                              {:type "element"})
                     :q
                     (map :value)
                     sort
                     (assoc op :type :ok, :value))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  {:client    (Client. nil)
   :checker   (checker/set)
   :generator (->> (range)
                   (map (fn [i] {:type :invoke, :f :add, :value i}))
                   gen/seq
                   (gen/stagger 1/10))
   :final-generator (gen/each (gen/once {:type :invoke, :f :read}))})


; This variant uses a single UID to store all values, and keeps a set of
; successfully written elements which we use to force a strong read.
(defrecord UidClient [conn uid written successfully-read?]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn "value: [int] .")
    (c/with-txn [t conn]
      (deliver uid (first (vals (c/mutate! t {:value -1}))))
      (info "UID is" @uid)))

  (invoke! [this test op]
    (c/with-conflict-as-fail op
      (case (:f op)
        :add (let [inserted (c/with-txn [t conn]
                              (c/mutate! t {:uid @uid
                                            :value (:value op)}))]
               (swap! written conj (:value op))
               (assoc op :type :ok, :uid @uid))

        :read (let [r (c/with-txn [t conn]
                        (let [found (->> (c/query t
                                                  (str "{ q(func: uid($u)) { "
                                                       "uid, value } }")
                                                  {:u @uid})
                                         :q
                                         (mapcat :value)
                                         (remove #{-1}) ; Our sentinel
                                         (into (sorted-set)))]
                          ; We need to force a conflict on every one tuple we
                          ; supposedly wrote, to ensure that we're not failing
                          ; to read because some other txn is still ongoing.
                          (doseq [w @written]
                            (info "Forcing conflict by"
                                  (if (found w) "inserting" "deleting")
                                  w)
                            ; We're going to write back exactly what we read,
                            ; so our read doesn't change anything
                            (if (found w)
                              ; We read this element
                              (c/mutate! t {:uid   @uid
                                            :value w})
                              ; We failed to read this element; to force a
                              ; conflict, we delete it.
                              (c/delete! t {:uid @uid, :value w})))
                          found))]
                (deliver successfully-read? true)
                (assoc op :type :ok, :value r)))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn uid-workload
  "A variant which stores every value associated with the same UID and avoids
  using any indices."
  [opts]
  (let [successfully-read? (promise)]
    (assoc (workload opts)
           :client (UidClient. nil (promise) (atom #{}) successfully-read?)
           :final-generator (->> (fn [_ _]
                                   (when-not (realized? successfully-read?)
                                     {:type :invoke, :f :read}))
                                 (gen/stagger 20)))))
