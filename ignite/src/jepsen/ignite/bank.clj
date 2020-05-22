(ns jepsen.ignite.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
    (:require [clojure.tools.logging   :refer :all]
              [jepsen [ignite          :as ignite]
                      [checker         :as checker]
                      [client          :as client]
                      [nemesis         :as nemesis]
                      [generator       :as gen]]
              [clojure.core.reducers :as r]
              [jepsen.checker.timeline :as timeline]
              [knossos.model           :as model]
              [knossos.op :as op])
    (:import  (org.apache.ignite Ignition)
      (org.apache.ignite.transactions TransactionConcurrency TransactionIsolation)
      (org.apache.ignite.transactions TransactionTimeoutException)
      (org.apache.ignite.cache CacheMode CacheAtomicityMode CacheWriteSynchronizationMode)))

(def accounts 10)
(def account-balance 100)

(def cache-name "ACCOUNTS")
(def amount 3)
(def n 10)

(def transaction-concurrency TransactionConcurrency/OPTIMISTIC)
(def transaction-isolation TransactionIsolation/SERIALIZABLE)

(defn init-cache [cache]
      (dotimes [i accounts]
               (error "Creating account" i)
               (.put cache i account-balance)))

(defn read-values [cache n]
      (vals (.getAll cache (set (range 0 n)))))

(defn transfer-money [from to cache]
      (let [b1 (- (.get cache from) amount)
            b2 (+ (.get cache to) amount)]
           (.put cache from b1)
           (.put cache to b2)))

(defn read-values-tr [ignite cache n]
      (with-open [tr (.txStart (.transactions ignite) transaction-concurrency transaction-isolation)]
                 (let [values (read-values cache n)]
                      (.commit tr)
                      values)))

(defn transfer-money-tr [ignite from to cache]
      (with-open [tr (.txStart (.transactions ignite) transaction-concurrency transaction-isolation)]
                 (transfer-money from to cache)
                 (.commit tr)))

(defn bank-checker
  "Balances must all be non-negative and sum to the model's total."
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [bad-reads (->> history
                        (r/filter op/ok?)
                        (r/filter #(= :read (:f %)))
                        (r/map (fn [op]
                          (let [balances (:value op)]
                            (cond
                              (not= accounts (count balances))
                              {:type :wrong-n
                               :expected accounts
                               :found    (count balances)
                               :op       op}

                              (not= (* accounts account-balance) (reduce + balances))
                              {:type :wrong-total
                               :expected (* accounts account-balance)
                               :found   (reduce + balances)
                               :op       op}

                              (some neg? balances)
                              {:type     :negative-value
                               :found    balances
                               :op       op}))))
                       (r/filter identity)
                       (into []))]
        {:valid? (empty? bad-reads)
         :bad-reads bad-reads}))))

(defrecord BankClient
  [cache-initialised?
   ignite-config-file
   conn
   cache-config
   transaction-config
   cache]
  client/Client
  (open! [this test node]
         (let [config (ignite/configure-client (:nodes test) (:pds test))
               conn (Ignition/start (.getCanonicalPath config))
               cache (.getOrCreateCache conn (ignite/getCacheConfiguration cache-config))]
              (assoc this :conn conn :cache cache :config config)))
  (setup! [this test]
    (locking cache-initialised?
      (when (compare-and-set! cache-initialised? false true)
        (init-cache cache))))
  (invoke! [_ test op]
    (try
      (case (:f op)
        :read (let [value (read-values cache n)]
                (assoc op :type :ok, :value value))
        :transfer (let [{:keys [from to amount]} (:value op)
                    amount (transfer-money from to cache)]
                    (assoc op :type :ok)))
      (catch Exception e (info (.getMessage e)) (assoc op :type :fail, :error :exception))))
  (teardown! [this test])
  (close! [this test]
    (.close conn)))

(defn bank-read
  "Reads the current state of all accounts without any synchronization."
  [_ _]
  {:type :invoke, :f :read})

(defn bank-transfer
  "Transfers a random amount between two randomly selected accounts."
  [_ _]
  {:type  :invoke
   :f     :transfer
   :value {:from (long (rand-int accounts))
           :to   (long (rand-int accounts))}})

(def bank-diff-transfer
  "Like transfer, but only transfers between *different* accounts."
  (gen/filter
  (fn [op]
    (not= (-> op :value :from)
          (-> op :value :to)))
  bank-transfer))

(defn test
  [opts]
  (ignite/basic-test
    (merge
      {:name      "bank-test"
       :client    (BankClient. (atom false) nil nil (ignite/get-cache-config opts) (ignite/get-transaction-config opts) nil)
       :checker   (checker/compose
                    {:perf     (checker/perf)
                     :timeline (timeline/html)
                     :details  (bank-checker)})
       :generator (ignite/generator [bank-diff-transfer bank-read] (:time-limit opts))}
      opts)))
