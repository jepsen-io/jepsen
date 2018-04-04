(ns jepsen.dgraph.sequential
  "Verifies sequentialish consistency by ensuring that each process observes
  monotonic states.

  Dgraph provides snapshot isolation, but snapshot isolation allows reads to
  observe arbitrarily stale values, so long as writes do not conflict. In
  particular, there is no guarantee that reads will observe logically monotonic
  states of the system. From https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf:

  > ... each transaction reads data from a snapshot of the (committed) data as
  > of the time the transaction started, called its Start-Timestamp. This time
  > may be any time before the transaction's first Read.

  We wish to verify a different sort of property, which I'm going to call
  *sequential consistency*, but isn't exactly.

  Sequential implies that there exists a total order of transactions which is
  consistent with the order on each process. Snapshot isolation, however, does
  not necessarily admit any totally ordered history; that would require
  serializability.

  What we're going to do here is restrict ourselves to transactions of two
  forms:

  1. Read-only transactions
  2. Transactions where every read key is also written

  I would like to argue that this is sufficient to imply serializability, by
  making hand-waving gestures and muttering about materializing conflicts, and
  observing that the example of a read-only nonserializable history in
  https://www.cs.umb.edu/~poneil/ROAnom.pdf requires a transaction which
  doesn't write its full read set.

  So, if we allow that a total transaction order *does* exist, then we can
  construct a transactional analogy for sequential consistency: there exists a
  total order of transactions which is compatible with the order of
  transactions on every given process.

  To check this, we perform two types of transactions on a register: a read
  transaction, and a read, increment, and write transaction. These are of type
  1 and type 2 respectively, so our histories ought to be serializable. Since
  no transaction can *lower* the value of the register, once a value is
  observed by a process, that process should observe that value or higher from
  that point forward.

  To verify this, we'll record the resulting value of the register in the
  :value for each operation, and ensure that in each process, those values are
  monotonic."
  (:require [clojure.tools.logging :refer [info]]
            [dom-top.core :refer [with-retry]]
            [knossos.op :as op]
            [jepsen.dgraph [client :as c]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [independent :as independent]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (c/alter-schema! conn (str "key:   int @index(int)"
                               (when (:upsert-schema test) " @upsert")
                               " .\n"
                               "value: int @index(int) .\n")))

  (invoke! [this test op]
    (let [[k _] (:value op)]
      (c/with-conflict-as-fail op
        (c/with-txn [t conn]
          (case (:f op)
            :inc (let [{:keys [uid value] :or {value 0}}
                       (->> (c/query t "{ q(func: eq(key, $key)) {
                                       uid, value
                                       }}"
                                     {:key k})
                            :q
                            first)
                       value (inc value)]
                   (if uid
                     (c/mutate! t {:uid uid, :value value})
                     (c/mutate! t {:key k,   :value value}))
                   (assoc op :type :ok, :value (independent/tuple k value)))

            :read (-> (c/query t "{ q(func: eq(key, $key)) { uid, value } }"
                                {:key k})
                       :q
                       first
                       :value
                       (or 0)
                       (->> (independent/tuple k)
                            (assoc op :type :ok, :value))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn checker
  "This checks a single register; we generalize it using independent/checker."
  []
  (reify checker/Checker
    (check [_ test model history opts]
      (let [errs (->> history
                      (filter op/ok?)
                      (reduce (fn [[last errs] op]
                                ; Last is a map of process ids to the last
                                ; operation we saw for that process. Errs is a
                                ; collection of error maps.
                                (let [p          (:process op)
                                      value      (:value op)
                                      last-value (-> (last p) :value (or 0))]
                                  (if (<= last-value value)
                                    ; Monotonic
                                    [(assoc last p op) errs]
                                    ; Non-monotonic!
                                    [(assoc last p op)
                                     (conj errs [(last p) op])])))
                              [{} []])
                      second)]
        {:valid? (empty? errs)
         :non-monotonic errs}))))

(defn inc-gen  [_ _]
  {:type :invoke, :f :inc, :value (independent/tuple (rand-int 8) nil)})
(defn read-gen [_ _]
  {:type :invoke, :f :read, :value (independent/tuple (rand-int 8) nil)})

(defn workload
  "Stuff you need to build a test!"
  [opts]
  {:client    (Client. nil)
   :checker   (independent/checker
                (checker/compose
                  {:sequential (checker)
                   :timeline   (timeline/html)}))
   :generator (gen/mix [inc-gen read-gen])})
