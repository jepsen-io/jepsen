(ns jepsen.cockroach-test
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [jepsen.core :as jepsen]
            [jepsen.control :as control]
            [jepsen.cockroach :as cl]
            [jepsen.cockroach [register :as register]]
            [jepsen.cockroach-nemesis :as cln]))

(def nodes [:n1l :n2l :n3l :n4l :n5l])

(defn check
  [test nemesis linearizable]
  (is (:valid? (:results (jepsen/run! (test nodes nemesis linearizable))))))

(defmacro def-split
  [name suffix base nemesis]
  `(do
     (deftest ~(symbol (str name suffix))        (check ~base ~nemesis false))
     (deftest ~(symbol (str name "-lin" suffix)) (check ~base ~nemesis true))
     ))


(defmacro def-tests
  [base]
  (let [name# (-> base (name) (string/replace "cl/" "") (string/replace "-test" ""))]
    `(do
       (def-split ~name# ""                    ~base cln/none)
       (def-split ~name# "-parts"              ~base cln/parts)
       (def-split ~name# "-majring"            ~base cln/majring)
       (def-split ~name# "-skews"              ~base cln/skews)
       (def-split ~name# "-bigskews"           ~base cln/bigskews)
       (def-split ~name# "-startstop"          ~base (cln/startstop 1))
       (def-split ~name# "-startstop2"         ~base (cln/startstop 2))
       ;;(def-split ~name# "-startkill"          ~base (cln/startkill 1))
       (def-split ~name# "-startkill2"         ~base (cln/startkill 2))
       (def-split ~name# "-skews-startkill2"   ~base (cln/compose cln/skews (cln/startkill 2)))
       (def-split ~name# "-majring-startkill2" ~base (cln/compose cln/majring (cln/startkill 2)))
       (def-split ~name# "-parts-skews"        ~base (cln/compose cln/parts   cln/skews))
       (def-split ~name# "-parts-bigskews"     ~base (cln/compose cln/parts   cln/bigskews))
       ;;(def-split ~name# "-parts-startkill"    ~base (cln/compose cln/parts   (cln/startkill 1)))
       (def-split ~name# "-parts-startkill2"   ~base (cln/compose cln/parts   (cln/startkill 2)))
       (def-split ~name# "-majring-skews"      ~base (cln/compose cln/majring cln/skews))
       (def-split ~name# "-startstop-skews"    ~base (cln/compose cln/startstop cln/skews))
       )))

(def-tests cl/atomic-test)
(def-tests cl/sets-test)
(def-tests cl/monotonic-test)
(def-tests cl/monotonic-multitable-test)
(def-tests cl/bank-test)
(def-tests cl/bank-multitable-test)
