(ns jepsen.model
  "Functional abstract models of database behavior."
  (:refer-clojure :exclude [set])
  (:import knossos.model.Model
           (clojure.lang PersistentQueue))
  (:use clojure.tools.logging)
  (:require [clojure.core :as core]
            [knossos.model :as knossos]
            [multiset.core :as multiset]))

(def inconsistent knossos/inconsistent)

(defrecord NoOp []
  Model
  (step [m op] m))

(def noop
  "A model which always returns itself, unchanged."
  (NoOp.))

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

(defn cas-register
  "A compare-and-set register"
  ([] (cas-register nil))
  ([value] (CASRegister. value)))

(defrecord Mutex [locked?]
  Model
  (step [r op]
    (condp = (:f op)
      :acquire (if locked?
                 (inconsistent "already held")
                 (Mutex. true))
      :release (if locked?
                 (Mutex. false)
                 (inconsistent "not held")))))

(defn mutex
  "A single mutex responding to :acquire and :release messages"
  []
  (Mutex. false))

(defrecord Set [s]
  Model
  (step [this op]
    (condp = (:f op)
      :add (Set. (conj s (:value op)))
      :read (if (= s (:value op))
              this
              (inconsistent (str "can't read " (pr-str (:value op)) " from "
                                 (pr-str s)))))))

(defn set
  "A set which responds to :add and :read."
  []
  (Set. #{}))

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
