(ns jepsen.txn
  "Manipulates transactions. Transactions are represented as a sequence of
  micro-operations (mops for short)."
  (:require [dom-top.core :refer [loopr]]))

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

(defn op-mops
  "A lazy sequence of all [op mop] pairs from a history."
  [history]
  (mapcat (fn [op] (map (fn [mop] [op mop]) (:value op))) history))

(defn reads
  "Given a transaction, returns a map of keys to sets of all values that
  transaction read."
  [txn]
  (loopr [reads (transient {})]
         [[f k v] txn]
         (if (= :r f)
           (let [vs (get reads k #{})]
             (recur (assoc! reads k (conj vs v))))
           (recur reads))
         (persistent! reads)))

(defn writes
  "Given a transaction, returns a map of keys to sets of all values that
  transaction wrote."
  [txn]
  (loopr [writes (transient {})]
         [[f k v] txn]
         (if (= :w f)
           (let [vs (get writes k #{})]
             (recur (assoc! writes k (conj vs v))))
           (recur writes))
         (persistent! writes)))

(defn ext-reads
  "Given a transaction, returns a map of keys to values for its external reads:
  values that transaction observed which it did not write itself."
  [txn]
  (loop [ext      (transient {})
         ignore?  (transient #{})
         txn      txn]
    (if (seq txn)
      (let [[f k v] (first txn)]
         (recur (if (or (not= :r f)
                        (ignore? k))
                  ext
                  (assoc! ext k v))
                (conj! ignore? k)
                (next txn)))
      (persistent! ext))))

(defn ext-writes
  "Given a transaction, returns the map of keys to values for its external
  writes: final values written by the txn."
  [txn]
  (loop [ext (transient {})
         txn txn]
    (if (seq txn)
      (let [[f k v] (first txn)]
        (recur (if (= :r f)
                 ext
                 (assoc! ext k v))
               (next txn)))
      (persistent! ext))))

(defn int-write-mops
  "Returns a map of keys to vectors of of all non-final write mops to that key."
  [txn]
  (loop [int (transient {})
         txn txn]
    (if (seq txn)
      (let [[f k v :as mop] (first txn)]
        (recur (if (= :r f)
                 int
                 (let [writes (get int k [])]
                   (assoc! int k (conj writes mop))))
               (next txn)))
      ; All done; trim final writes.
      (->> int
           persistent!
           (keep (fn [[k vs]]
                   (when (< 1 (count vs))
                     [k (subvec vs 0 (dec (count vs)))])))
           (into {})))))
