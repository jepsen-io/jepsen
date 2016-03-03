(ns jepsen.cockroach-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [jepsen.core :as jepsen]
            [jepsen.control :as control]
            [jepsen.cockroach :as cl]
            [jepsen.cockroach-nemesis :as cln]))

(def nodes [:n1l :n2l :n3l :n4l :n5l])

(defn check
  [test nemesis]
  (is (:valid? (:results (jepsen/run! (test nodes nemesis))))))

(defmacro def-tests
  [base]
  (let [name# (string/replace (name base) "cl/" "")]
  `(do
     (deftest ~(symbol name#)                        (check ~base cln/none))
     (deftest ~(symbol (str name# "-parts"))         (check ~base cln/parts))
     (deftest ~(symbol (str name# "-majring"))       (check ~base cln/majring))
     (deftest ~(symbol (str name# "-skews"))         (check ~base cln/skews))
     (deftest ~(symbol (str name# "-bigskews"))      (check ~base cln/bigskews))
     (deftest ~(symbol (str name# "-startstop"))     (check ~base cln/startstop))
     (deftest ~(symbol (str name# "-parts-skews"))   (check ~base (cln/compose cln/parts   cln/skews)))
     (deftest ~(symbol (str name# "-parts-bigskews")) (check ~base (cln/compose cln/parts   cln/bigskews)))
     (deftest ~(symbol (str name# "-majring-skews")) (check ~base (cln/compose cln/majring cln/skews)))
     (deftest ~(symbol (str name# "-startstop-skews")) (check ~base (cln/compose cln/startstop cln/skews)))
     )))

(def-tests cl/atomic-test)
(def-tests cl/sets-test)
(def-tests cl/monotonic-test)
(def-tests cl/monotonic-multitable-test)
(def-tests cl/bank-test)
