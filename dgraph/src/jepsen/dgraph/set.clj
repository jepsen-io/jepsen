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
    (c/alter-schema! conn (str "type: string @index(exact) .\n"
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
