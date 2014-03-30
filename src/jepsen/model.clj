(ns jepsen.model
  "Functional abstract models of database behavior."
  (:import knossos.core.Model)
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

(defrecord UnorderedQueue [pending]
  Model
  (step [r op]
    (condp = (:f op)
      :enqueue (UnorderedQueue. (conj pending (:value op)))
      :dequeue (if (contains? pending (:value op))
                 (UnorderedQueue. (disj pending (:value op)))
                 (inconsistent (str "can't dequeue " (:value op)
                                    " from " pending))))))

(defn unordered-queue
  "A queue which does not order its pending elements."
  []
  (UnorderedQueue. (multiset/multiset)))
