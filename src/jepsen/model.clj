(ns jepsen.model
  "Functional abstract models of database behavior."
  (:import knossos.core.Model)
  (:require [knossos.core :as knossos]))

(def inconsistent knossos/inconsistent)

(defrecord CASRegister [value]
  Model
  (step [r op]
    (condp = (:f op)
      :write (CASRegister. (:value op))
      :cas   (let [[cur new] (:value op)]
               (if (= cur value)
                 (CASRegister. new)
                 (knossos/inconsistent (str "can't CAS " value " from " cur
                                            " to " new))))
      :read  (if (or (nil? (:value op))
                     (= value (:value op)))
               r
               (knossos/inconsistent (str "can't read " (:value op)
                                          " from register " value))))))

(def noop
  "A model which always returns itself, unchanged."
  (reify Model
    (step [r op] r)))
