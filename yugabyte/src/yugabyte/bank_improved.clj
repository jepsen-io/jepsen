(ns yugabyte.bank-improved
  "Reworked original bank workload that now include inserts and deletes.
  Generator now throws dice in [:insert :delete :update]

  :update behaves as default bank workload operation.

  :insert there is 2 cases:
  for YCQL (update-insert) insert key will be appended to the end of the list
  for YSQL (update-insert-delete) inserted key will be chosen from contention-keys

  :delete
  for YCQL (update-insert) there is no way to transactional delete key
  for YSQL (update-insert-delete) deleted key will be chosen from contention-keys"
  (:refer-clojure :exclude
                  [test])
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info]]
            [jepsen.tests.bank :as bank]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [jepsen
             [generator :as gen]
             [checker :as checker]
             [util :as util]]))

(def start-key 0)
(def end-key 5)

(def insert-key-ctr (atom end-key))
(def contention-keys (range end-key (+ end-key 3)))

(defn transfer-without-deletes
  "Based on from original jepsen.tests.bank.transfer generator.

  Generator of a transfer: a random amount between two randomly selected accounts.
  Added insert operation. Special case for YCQL"
  [test process]
  (let [dice (rand-nth [:insert :update])]
    (cond
      (= dice :insert)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (:accounts test))
               :to     (swap! insert-key-ctr inc)
               :amount (+ 1 (rand-int (:max-transfer test)))}}

      (= dice :update)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (:accounts test))
               :to     (rand-nth (:accounts test))
               :amount (+ 1 (rand-int (:max-transfer test)))}})))

(defn transfer-contention-keys
  "Based on from original jepsen.tests.bank.transfer generator.

  Generator of a transfer: a random amount between two randomly selected
  accounts.
  A random amount between two randomly selected accounts with set of contention keys
  that may be inserted, deleted or updated."
  [test process]
  (let [dice (rand-nth (:operations test))]
    (cond
      (= dice :insert)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (:accounts test))
               :to     (rand-nth contention-keys)
               :amount (+ 1 (rand-int (:max-transfer test)))}}

      (= dice :update)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth (concat (:accounts test) contention-keys))
               :to     (rand-nth (concat (:accounts test) contention-keys))
               :amount (+ 1 (rand-int (:max-transfer test)))}}

      (= dice :delete)
      {:type  :invoke
       :f     dice
       :value {:from   (rand-nth contention-keys)
               :to     (rand-nth (:accounts test))
               :amount (+ 1 (rand-int (:max-transfer test)))}})))

(def diff-transfer-insert
  "Based on from original jepsen.tests.bank workload

  Transfers only between different accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer-without-deletes))

(def diff-transfer-contention
  "Based on from original jepsen.tests.bank workload

  Transfers only between different accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              transfer-contention-keys))

(defn check-op
  "Based on code from original jepsen.test.bank/check-op
  Here we need to exclude :negative-value and :unexpected-key checks"
  [accts total op]
  (let [ks (keys (:value op))
        balances (vals (:value op))]
    (cond
      (some nil? balances)
      {:type :nil-balance
       :nils (->> (:value op)
                  (remove val)
                  (into {}))
       :op   op}

      (not= total (reduce + balances))
      {:type  :wrong-total
       :total (reduce + balances)
       :op    op})))

(defn checker
  "Based on code from original jepsen.test.bank/checker
  Since we have internal check-op call this function needs to be modified"
  [checker-opts]
  (reify
    checker/Checker
    (check [this test history opts]
      (let [accts (set (:accounts test))
            total (:total-amount test)
            reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %))))
            errors (->> reads
                        (r/map
                          (partial check-op
                                   accts
                                   total))
                        (r/filter identity)
                        (group-by :type))]
        {:valid?      (every? empty? (vals errors))
         :read-count  (count (into [] reads))
         :error-count (reduce + (map count (vals errors)))
         :first-error (util/min-by (comp :index :op) (map first (vals errors)))
         :errors      (->> errors
                           (map
                             (fn [[type errs]]
                               [type
                                (merge
                                  {:count (count errs)
                                   :first (first errs)
                                   :worst (util/max-by
                                            (partial bank/err-badness test)
                                            errs)
                                   :last  (peek errs)}
                                  (if (= type :wrong-total)
                                    {:lowest  (util/min-by :total errs)
                                     :highest (util/max-by :total errs)}
                                    {}))]))
                           (into {}))}))))

(defn workload-with-inserts
  [opts]
  {:max-transfer 5
   :total-amount 100
   :accounts     (vec (range end-key))
   :checker      (checker/compose
                   {:SI   (checker opts)
                    :plot (bank/plotter)})
   :generator    (gen/mix [diff-transfer-insert
                           bank/read])})

(defn workload-contention-keys
  [opts]
  {:max-transfer 5
   :total-amount 100
   :accounts     (vec (range end-key))
   :operations   [:insert :update :delete]
   :checker      (checker/compose
                   {:SI   (checker opts)
                    :plot (bank/plotter)})
   :generator    (gen/mix [diff-transfer-contention
                           bank/read])})
