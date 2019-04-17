(ns jepsen.txn
  "Manipulates transactions. Transactions are represented as a sequence of
  micro-operations (mops for short).")

(defn ext-reads
  "Given a transaction, returns a map of keys to values for its external reads:
  values that transaction observed which it did not write itself."
  [txn]
  (->> txn
       (reduce (fn [[ext ignore?] [f k v]]
                 [(if (or (= :w f)
                          (ignore? k))
                    ext
                    (assoc ext k v))
                  (conj ignore? k)])
               [{} #{}])
       first))

(defn ext-writes
  "Given a transaction, returns the map of keys to values for its external
  writes: final values written by the txn."
  [txn]
  (reduce (fn [ext [f k v]]
            (if (= :r f)
              ext
              (assoc ext k v)))
          {}
          txn))
