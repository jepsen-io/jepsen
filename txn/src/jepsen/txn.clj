(ns jepsen.txn
  "Manipulates transactions. Transactions are represented as a sequence of
  micro-operations (mops for short).")

(defn reduce-mops
  "Takes a history of operations, where each operation op has a :value which is
  a transaction made up of [f k v] micro-ops. Runs a reduction over every
  micro-op, where the reduction function is of the form (f state op [f k v]).
  Saves you having to do endless nested reduces."
  [f init-state history]
  (reduce (fn op [state op]
            (reduce (fn mop [state mop]
                      (f state op mop))
                    state
                    (:value op)))
          init-state
          history))

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
