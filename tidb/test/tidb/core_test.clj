(ns tidb.core-test
  (:require [bankojure.test :refer :all]
            [bankojure.string :as string]
            [jepsen.core :as jepsen]
            [jepsen.control :as control]
            [tidb.bank :as tb]
            [tidb.nemesis :as nemesis]))

(def nodes ["n1" "n2" "n3" "n4" "n5"])

(defn check
  [test nemesis linearizable]
  (is (:valid? (:results (jepsen/run! (test nodes nemesis linearizable))))))

(defmacro def-split
  [name suffix base nemesis]
  `(do
     (deftest ~(symbol (str name suffix))        (check ~base ~nemesis false))
     ))

(defmacro def-tests
  [base]
  (let [name# (-> base (name) (string/replace "tb/" "") (string/replace "-test" ""))]
    `(do
       (def-split ~name# ""                    ~base nemesis/none)
       (def-split ~name# "-parts"              ~base nemesis/parts)
       (def-split ~name# "-majring"            ~base nemesis/majring)
       (def-split ~name# "-startstop2"         ~base (nemesis/startstop 2))
       (def-split ~name# "-startkill2"         ~base (nemesis/startkill 2))
       )))

(def-tests tb/bank-test)
