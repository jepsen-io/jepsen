(ns yugabyte.ysql.bank-improved
  (:require [version-clj.core :as v]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer [info]]
            [clojure.tools.logging :refer [debug info warn]]
            [yugabyte.ysql.client :as c]))

(def table-name "accounts")
(def table-index "idx_accounts")
(def enable-follower-reads true)
(def minimal-follower-read-version "2.8.0.0-b1")

;
; Single-table bank improved test
;

(defn- read-accounts-map
  "Read {id balance} accounts map from a unified bank table using force index flag"
  ([test op c]
   (if (and enable-follower-reads (v/newer-or-equal? (:version test) minimal-follower-read-version))
     (c/execute! c ["SET yb_read_from_followers = true"]))
   (->>
     (str "/*+ IndexOnlyScan(" table-name " " table-index ") */ SELECT id, balance FROM " table-name)
     (c/query op c)
     (map (juxt :id :balance))
     (into (sorted-map)))))


(defrecord YSQLBankContentionYBClient [isolation]
  c/YSQLYbClient

  (setup-cluster! [this test c conn-wrapper]
    (c/execute! c
                (j/create-table-ddl table-name
                                    [[:id :int "PRIMARY KEY"]
                                     [:balance :bigint]]))
    (c/execute! c [(str "CREATE INDEX " table-index " ON " table-name " (id, balance)")])
    (c/with-retry
      (info "Creating accounts")
      (c/insert! c table-name
                 {:id      (first (:accounts test))
                  :balance (:total-amount test)})
      (doseq [acct (rest (:accounts test))]
        (c/insert! c table-name
                   {:id      acct,
                    :balance 0}))))

  (invoke-op! [this test op c conn-wrapper]
    (case (:f op)
      :read
      (j/with-db-transaction [c c {:isolation isolation}]
        (assoc op :type :ok, :value (read-accounts-map test op c)))

      :update
      (j/with-db-transaction [c c {:isolation isolation}]
        (let [{:keys [from to amount]} (:value op)
              b-from-before (c/select-single-value op c table-name :balance (str "id = " from))
              b-to-before (c/select-single-value op c table-name :balance (str "id = " to))]
          (cond
            (or (nil? b-from-before) (nil? b-to-before))
            (assoc op :type :fail)

            :else
            (let [b-from-after (- b-from-before amount)
                  b-to-after (+ b-to-before amount)]
              (do
                (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
                (c/update! op c table-name {:balance b-to-after} ["id = ?" to])
                (assoc op :type :ok))))))

      :delete
      (j/with-db-transaction [c c {:isolation isolation}]
        (let [{:keys [from to amount]} (:value op)
              b-from-before (c/select-single-value op c table-name :balance (str "id = " from))
              b-to-before (c/select-single-value op c table-name :balance (str "id = " to))]
          (cond
            (or (nil? b-from-before) (nil? b-to-before))
            (assoc op :type :fail)

            :else
            (let [b-to-after-delete (+ b-to-before b-from-before)]
              (do
                (c/execute! op c [(str "DELETE FROM " table-name " WHERE id = ?") from])
                (c/update! op c table-name {:balance b-to-after-delete} ["id = ?" to])
                (assoc op :type :ok :value {:from from, :to to, :amount b-from-before}))))))

      :insert
      (j/with-db-transaction [c c {:isolation isolation}]
        (let [{:keys [from to amount]} (:value op)
              b-from-before (c/select-single-value op c table-name :balance (str "id = " from))
              b-to-before (c/select-single-value op c table-name :balance (str "id = " to))]
          (cond
            (or (nil? b-from-before) (not (nil? b-to-before)))
            (assoc op :type :fail)

            :else
            (let [b-from-after (- b-from-before amount)]
              (do
                (c/update! op c table-name {:balance b-from-after} ["id = ?" from])
                (c/insert! op c table-name {:id to :balance amount})
                (assoc op :type :ok :value {:from from, :to to, :amount amount}))))))))

  (teardown-cluster! [this test c conn-wrapper]
    (c/drop-table c table-name)))

(c/defclient YSQLBankContentionClient YSQLBankContentionYBClient)
