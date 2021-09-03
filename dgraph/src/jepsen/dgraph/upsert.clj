(ns jepsen.dgraph.upsert
  "Attempts to upsert documents based on an index read, and verifies that at
  most one upsert succeeds per key."
  (:require [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [with-retry]]
            [knossos.op :as op]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [independent :as independent]]
            [jepsen.generator.pure :as gen]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "email: string @index(exact)"
                               (when (:upsert-schema test) " @upsert")
                               " .")))

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (c/with-conflict-as-fail op
        (c/with-txn [t conn]
          (case (:f op)
            ; TODO: I broke this by changing client/upsert to an actual upsert,
            ; rather than insert-unless-exists. We should replace this.
            :upsert (let [inserted (c/upsert! t
                                              :email
                                              {:email (str k)})]
                      (assoc op
                             :type  (if inserted :ok :fail)
                             :value (independent/tuple
                                      k (first (vals inserted)))))

            :read (->> (c/query t (str "{\n"
                                       "  q(func: eq(email, $email)) {\n"
                                       "    uid\n"
                                       "  }\n"
                                       "}")
                                {:email (str k)})
                       :q
                       (map :uid)
                       sort
                       (independent/tuple k)
                       (assoc op :type :ok, :value)))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn checker
  "Ensures that at most one UID is ever returned from any read."
  []
  (reify checker/Checker
    (check [this test history opts]
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
   :checker   (independent/checker (checker))
   :generator (independent/pure-concurrent-generator
                (min (:concurrency opts)
                     (* 2 (count (:nodes opts))))
                (range)
                (fn [k]
                  (gen/phases (gen/each-thread {:f :upsert})
                              (gen/each-thread {:f :read}))))})
