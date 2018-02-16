(ns jepsen.dgraph.upsert
  "Attempts to upsert documents based on an index read, and verifies that at
  most one upsert succeeds per key."
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
    (c/alter-schema! conn "email: string @index(exact) ."))

  (invoke! [this test op]
    (c/with-conflict-as-fail op
      (c/with-txn [t conn]
        (case (:f op)
          :upsert (let [inserted (c/upsert! t
                                            :email
                                            {:email "bob@example.com"})]
                    (assoc op
                           :type  (if inserted :ok :fail)
                           :value (first (vals inserted))))

          :read (->> (str "{\n"
                          "  q(func: eq(email, \"bob@example.com\")) {\n"
                          "    uid\n"
                          "  }\n"
                          "}")
                     (c/query t)
                     :q
                     (map :uid)
                     sort
                     (assoc op :type :ok, :value))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn checker
  "Ensures that at most one UID is ever returned from any read."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [reads       (->> history
                             (filter op/ok?)
                             (filter #(= :read (:f %))))
            upserts     (->> history
                             (filter op/ok?)
                             (filter #(= :upsert (:f %))))
            bad-reads   (filter #(< 1 (count (:value %))) reads)]
        {:valid?      (and (empty? bad-reads)
                           (<= (count upserts) 1))
         :bad-reads   bad-reads
         :ok-upserts  upserts}))))

(defn workload
  "Stuff you need to build a test!"
  [opts]
  {:client    (Client. nil)
   :checker   (checker)
   :generator (gen/phases (gen/each (gen/once {:type :invoke, :f :upsert}))
                          (gen/each (gen/once {:type :invoke, :f :read})))})
