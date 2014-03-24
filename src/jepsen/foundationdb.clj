(ns jepsen.foundationdb
  (:use [clojure.set :only [union difference]]
        jepsen.util
        jepsen.set-app
        clojure.string)
  (:import (com.foundationdb FDB))
  (:import (com.foundationdb.async Function))
  (:import (com.foundationdb.tuple Tuple)))

  (def getlist
    "Returns the value of 'key' in the database, as a list."
    (reify Function
      (apply [this, tr]
          (map #(Long. %) (split (String. (or (.get (.get tr (byte-array (map byte "key")))) "")) #",")))))

  (def getall
    "Returns all the values in the database, as Long types."
    (reify Function
      (apply [this, tr]
        (def values (.iterator (.getRange tr (.pack (Tuple/from (into-array [""]))) (.pack (Tuple/from (into-array ["xFF"]))))))
        (map (fn [value] 
          (.getLong (Tuple/fromBytes (.getValue value)) 0)
        ) (iterator-seq values)))))

  (def clearall
    "Deletes all the data from the database."
    (reify Function
      (apply [this, tr]
        (.clear tr (.pack (Tuple/from (into-array [""]))) (.pack (Tuple/from (into-array ["xFF"])))))))

(defn foundationdb-app
  "Creates a new key/value pair for each element, and uses the automatic transaction retry loop."
  [opts]
    (reify SetApp
      (setup [app]
        (def fdb (FDB/selectAPIVersion 200))
        (def db (.open fdb)))

      (add [app element]
        (let [setter 
          (reify Function
            (apply [this, tr]
              (let [
                key_var (.pack (Tuple/from (into-array [(Integer/toString element)])))
                value_var (.pack (Tuple/from (into-array [element])))]
              (.set tr key_var value_var))))]

          (.run db setter)))
      (results [app]
        (set (map #(Long. %) (.run db getall))))

      (teardown [app]
        (.run db clearall))))

(defn foundationdb-append-app
  "Adds each element to a list, and uses the automatic transaction retry loop."
  [opts]
    (reify SetApp
      (setup [app]
            (def fdb (FDB/selectAPIVersion 200))
            (def db (.open fdb)))

      (add [app element]
            (let [setter 
              (reify Function
                (apply [this, tr]
                  (let [
                      key (byte-array (map byte "key"))
                      value (str (String. (or (.get (.get tr key)) "")) element ",")]

                    (.set tr key (byte-array (map byte value))))))]

            (.run db setter)))

      (results [app]
        (.run db getlist))

      (teardown [app]
        (.run db clearall))))

(defn foundationdb-append-noretry-app
  "Adds each element to a list, and does not retry failed transactions."
  [opts]
    (reify SetApp
      (setup [app]
            (def fdb (FDB/selectAPIVersion 200))
            (def db (.open fdb)))

      (add [app element]
        (let [
            tr (.createTransaction db)
            key (byte-array (map byte "key"))
            value (str (String. (or (.get (.get tr key)) "")) element ",")]

          (.set tr key (byte-array (map byte value)))
          (.get (.commit tr))
          {}))

      (results [app]
        (.run db getlist))

      (teardown [app]
        (.run db clearall))))