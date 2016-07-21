(ns jepsen.cockroach.register
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
  (:require [jepsen [cockroach :as c]
             [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]]
            [clojure.java.jdbc :as :j]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]))

(defrecord BankClient [tbl-created? n starting-balance]
  client/Client 
  (setup! [this test node]
    (let [conn (init-conn node)]

      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (Thread/sleep 1000) 
          (with-txn-notimeout {} [c conn] (j/execute! c ["drop table if exists accounts"]))
          (Thread/sleep 1000)
          (info "Creating table")
          (with-txn-notimeout {} [c conn] (j/execute! c ["create table accounts (id int not null primary key, balance bigint not null)"]))
          (dotimes [i n]
            (Thread/sleep 500)
            (info "Creating account" i)
            (with-txn-notimeout {} [c conn] (j/insert! c :accounts {:id i :balance starting-balance})))))

      (assoc this :conn conn))
    )

  (invoke! [this test op]
    (let [conn (:conn this)]
      (with-txn op [c conn]
        (case (:f op)
          :read (->> (j/query c ["select balance from accounts"])
                     (mapv :balance)
                     (assoc op :type :ok, :value))

          :transfer (let [{:keys [from to amount]} (:value op)
                          b1 (-> c
                                 (j/query ["select balance from accounts where id = ?" from]
                                          :row-fn :balance)
                                 first
                                 (- amount))
                          b2 (-> c
                                 (j/query ["select balance from accounts where id = ?" to]
                                          :row-fn :balance)
                                 first
                                 (+ amount))]
                      (cond (neg? b1)
                            (assoc op :type :fail, :value [:negative from b1])

                            (neg? b2)
                            (assoc op :type :fail, :value [:negative to b2])

                            true
                            (do (j/update! c :accounts {:balance b1} ["id = ?" from])
                                (j/update! c :accounts {:balance b2} ["id = ?" to])
                                (assoc op :type :ok))))
          ))))

  (teardown! [this test]
    (let [conn (:conn this)]
      (meh (with-timeout conn nil
             (j/execute! @conn ["drop table if exists accounts"])))
      (close-conn @conn)))
  )

(defn bank-read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [test process]
  (let [n (-> test :client :n)]
    {:type  :invoke
     :f     :transfer
     :value {:from   (rand-int n)
             :to     (rand-int n)
             :amount (+ 1 (rand-int 5))}}))

(def bank-diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter (fn [op] (not= (-> op :value :from)
                             (-> op :value :to)))
              bank-transfer))
(defn bank-checker
    "Balances must all be non-negative and sum to the model's total."
      []
        (reify checker/Checker
              (check [this test model history opts]
                      (let [bad-reads (->> history
                                                                      (r/filter op/ok?)
                                                                                                 (r/filter #(= :read (:f %)))
                                                                                                                            (r/map (fn [op]
                                                                                                                                                                       (let [balances (:value op)]
                                                                                                                                                                                                             (cond (not= (:n model) (count balances))
                                                                                                                                                                                                                                                             {:type :wrong-n
                                                                                                                                                                                                                                                                                                         :expected (:n model)
                                                                                                                                                                                                                                                                                                                                                    :found    (count balances)
                                                                                                                                                                                                                                                                                                                                                                                               :op       op}

                                                                                                                                                                                                                                                                                                      (not= (:total model)
                                                                                                                                                                                                                                                                                                                                                           (reduce + balances))
                                                                                                                                                                                                                                                                                                                                               {:type :wrong-total
                                                                                                                                                                                                                                                                                                                                                                                          :expected (:total model)
                                                                                                                                                                                                                                                                                                                                                                                                                                    :found    (reduce + balances)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                              :op       op}

                                                                                                                                                                                                                                                                                                                                                                                        (some neg? balances)
                                                                                                                                                                                                                                                                                                                                                                                                                                 {:type     :negative-value
                                                                                                                                                                                                                                                                                                                                                                                                                                                                            :found    balances
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      :op       op}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                          ))))
                                                                                                                                                       (r/filter identity)
                                                                                                                                                                                  (into []))]
                                {:valid? (empty? bad-reads)
                                          :bad-reads bad-reads}))))

(defn bank-test-base
    [name-suffix client nodes nemesis linearizable]
      (basic-test nodes nemesis linearizable
                      {:name        (str "bank" name-suffix)
                            :concurrency concurrency-factor
                                 :model       {:n 4 :total 40}
                                      :client      client
                                           :generator   (gen/phases
                                                                                       (->> (gen/mix [bank-read bank-diff-transfer])
                                                                                                                    (gen/clients)
                                                                                                                                            (gen/stagger 1)
                                                                                                                                                                    (cln/with-nemesis (:generator nemesis)))
                                                                                                          (gen/clients (gen/once bank-read)))
                                                :checker     (checker/compose
                                                                                                 {:perf (checker/perf)
                                                                                                                      :details (bank-checker)})}))

(defn bank-test
  [nodes nemesis linearizable]
  (bank-test-base "" (BankClient. (atom false) 4 10) nodes nemesis linearizable))

(defrecord MultiBankClient [tbl-created? n starting-balance]
  client/Client
  (setup! [this test node]
    (let [conn (init-conn node)]

      (locking tbl-created?
        (when (compare-and-set! tbl-created? false true)
          (dotimes [i n]
            (Thread/sleep 500)
            (with-txn-notimeout {} [c conn] (j/execute! c [(str "drop table if exists accounts" i)]))
            (Thread/sleep 500)
            (info "Creating table " i)
            (with-txn-notimeout {} [c conn] (j/execute! c [(str "create table accounts" i " (balance bigint not null)")]))
            (Thread/sleep 500)
            (info "Populating account" i)
            (with-txn-notimeout {} [c conn] (j/insert! c (str "accounts" i) {:balance starting-balance})))))

      (assoc this :conn conn))
    )

  (invoke! [this test op]
    (let [conn (:conn this)]
      (with-txn op [c conn]
        (case (:f op)
          :read (->> (range n)
                     (map (fn [x]
                            (->> (j/query c [(str "select balance from accounts" x)] :row-fn :balance)
                                 first)))
                     (into [])
                     (assoc op :type :ok, :value))

          :transfer (let [{:keys [from to amount]} (:value op)
                          b1 (-> c
                                 (j/query [(str "select balance from accounts" from)]
                                          :row-fn :balance)
                                 first
                                 (- amount))
                          b2 (-> c
                                 (j/query [(str "select balance from accounts" to)]
                                          :row-fn :balance)
                                 first
                                 (+ amount))]
                      (cond (neg? b1)
                            (assoc op :type :fail, :value [:negative from b1])

                            (neg? b2)
                            (assoc op :type :fail, :value [:negative to b2])

                            true
                            (do (j/update! c (str "accounts" from) {:balance b1} [])
                                (j/update! c (str "accounts" to) {:balance b2} [])
                                (assoc op :type :ok))))
          ))))

  (teardown! [this test]
    (let [conn (:conn this)]
      (meh (with-timeout conn nil
             (dotimes [i n]
               (j/execute! @conn [(str "drop table if exists accounts" i)]))))
      (close-conn @conn)))
  )

(defn bank-multitable-test
  [nodes nemesis linearizable]
  (bank-test-base "-multitable" (MultiBankClient. (atom false) 4 10) nodes nemesis linearizable))

