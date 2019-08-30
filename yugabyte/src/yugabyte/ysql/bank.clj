(ns yugabyte.ysql.bank
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen.client :as client]
            [jepsen.reconnect :as rc]
            [yugabyte.ysql.client :as c]))

(def table-name "accounts")

;
; Single-table bank test
;

(defn- read-accounts-map
  "Read {id balance} accounts map from a unified bank table"
  [op c]
  (->> (str "SELECT id, balance FROM " table-name)
       (c/query op c)
       (map (juxt :id :balance))
       (into (sorted-map))))

(defrecord YSQLBankYbClient [allow-negatives?]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c (j/create-table-ddl table-name [[:id :int "PRIMARY KEY"]
                                                  [:balance :bigint]]))
    (c/with-retry
      (info "Creating accounts")
      (c/insert! c table-name {:id      (first (:accounts test))
                               :balance (:total-amount test)})
      (doseq [acct (rest (:accounts test))]
        (c/insert! c table-name {:id      acct,
                                 :balance 0}))))


  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :read
      (assoc op :type :ok, :value (read-accounts-map op c))

      :transfer
      (c/with-txn
        c
        (let [{:keys [from to amount]} (:value op)]
          (let [b-from-before (c/select-single-value op c table-name :balance (str "id = " from))
                b-to-before   (c/select-single-value op c table-name :balance (str "id = " to))
                b-from-after  (- b-from-before amount)
                b-to-after    (+ b-to-before amount)
                allowed?      (or allow-negatives? (pos? b-from-after))]
            (if (not allowed?)
              (assoc op :type :fail, :error [:negative from b-from-after])
              (do (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
                  (c/update! op c table-name {:balance b-to-after} ["id = ?" to])
                  (assoc op :type :ok))))))))


  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))


(c/defclient YSQLBankClient YSQLBankYbClient)


;
; Multi-table bank test
;

(defrecord YSQLMultiBankYbClient [allow-negatives?]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]

    (doseq [a (:accounts test)]
      (let [acc-table-name (str table-name a)
            balance        (if (= a (first (:accounts test)))
                             (:total-amount test)
                             0)]
        (info "Creating table" a)
        (c/execute! c (j/create-table-ddl acc-table-name [[:id :int "PRIMARY KEY"]
                                                          [:balance :bigint]]))

        (info "Populating account" a " (balance =" balance ")")
        (c/with-retry
          (c/insert! c acc-table-name {:id      a
                                       :balance balance})))))


  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :read
      (c/with-txn
        c
        (let [accs (shuffle (:accounts test))]
          (->> accs
               (mapv (fn [a]
                       (c/select-single-value op c (str table-name a) :balance (str "id = " a))))
               (zipmap accs)
               (assoc op :type :ok, :value))))

      :transfer
      (let [{:keys [from to amount]} (:value op)]
        (c/with-txn
          c
          (let [b-from-before (c/select-single-value op c (str table-name from) :balance (str "id = " from))
                b-to-before   (c/select-single-value op c (str table-name to) :balance (str "id = " to))
                b-from-after  (- b-from-before amount)
                b-to-after    (+ b-to-before amount)
                allowed?      (or allow-negatives? (pos? b-from-after))]
            (if (not allowed?)
              (assoc op :type :fail, :error [:negative from b-from-after])
              (do (c/update! op c (str table-name from) {:balance b-from-after} ["id = ?" from])
                  (c/update! op c (str table-name to) {:balance b-to-after} ["id = ?" to])
                  (assoc op :type :ok))))))))


  (teardown-cluster! [this test c conn-wrapper]
    (doseq [a (:accounts test)]
      (c/drop-table c (str table-name a)))))


(c/defclient YSQLMultiBankClient YSQLMultiBankYbClient)
