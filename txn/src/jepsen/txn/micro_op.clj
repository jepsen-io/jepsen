(ns jepsen.txn.micro-op
  "Transactions are made up of micro-operations. This namespace helps us work
  with those."
  (:refer-clojure :exclude [key val]))

(defn f
  "What function is this op executing?"
  [op]
  (nth op 0))

(defn key
  "What key did this op affect?"
  [op]
  (nth op 1))

(defn value
  "What value did this op use?"
  [op]
  (nth op 2))

(defn read?
  "Is the given operation a read?"
  [op]
  (= :r (f op)))

(defn write?
  "Is the given operation a write?"
  [op]
  (= :w (f op)))

(defn op?
  "Is this a legal operation?"
  [op]
  (and (= 3 (count op))
       (#{:r :w} (f op))))
