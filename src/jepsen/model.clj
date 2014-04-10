(ns jepsen.model
  "Functional abstract models of database behavior."
  (:import knossos.core.Model
           (clojure.lang PersistentQueue))
  (:use clojure.tools.logging)
  (:require [knossos.core :as knossos]
            [multiset.core :as multiset]))

(def inconsistent knossos/inconsistent)

(def noop
  "A model which always returns itself, unchanged."
  (reify Model
    (step [r op] r)))

(defrecord CASRegister [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (CASRegister. (:value op))
      :cas   (let [[cur new] (:value op)]
               (if (= cur value)
                 (CASRegister. new)
                 (inconsistent (str "can't CAS " value " from " cur
                                    " to " new))))
      :read  (if (or (nil? (:value op))
                     (= value (:value op)))
               r
               (inconsistent (str "can't read " (:value op)
                                  " from register " value))))))

(defrecord Mutex [locked]
  Model
  (step [r op]
    (condp = (:f op)
      :acquire (if locked
                 (inconsistent "already held")
                 (Mutex. true))
      :release (if locked
                 (Mutex. false)
                 (inconsistent "not held")))))

(defn mutex
  "A single mutex responding to :acquire and :release messages"
  []
  (Mutex. false))

(defrecord UnorderedQueue [pending]
  Model
  (step [r op]
    (condp = (:f op)
      :enqueue (UnorderedQueue. (conj pending (:value op)))
      :dequeue (if (contains? pending (:value op))
                 (UnorderedQueue. (disj pending (:value op)))
                 (inconsistent (str "can't dequeue " (:value op)))))))

(defn unordered-queue
  "A queue which does not order its pending elements."
  []
  (UnorderedQueue. (multiset/multiset)))

(defrecord FIFOQueue [pending]
  Model
  (step [r op]
    (condp = (:f op)
      :enqueue (FIFOQueue. (conj pending (:value op)))
      :dequeue (cond (zero? (count pending))
                     (inconsistent (str "can't dequeue " (:value op)
                                        " from empty queue"))

                     (= (:value op) (peek pending))
                     (FIFOQueue. (pop pending))

                     :else
                     (inconsistent (str "can't dequeue " (:value op)))))))

(defn fifo-queue
  "A FIFO queue."
  []
  (FIFOQueue. PersistentQueue/EMPTY))
