(ns yugabyte.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
  (:require [jepsen.tests.bank :as bank]
            [yugabyte.generator :as ygen]))

(defn workload
  [opts]
  (ygen/workload-with-op-index (bank/test)))

(defn workload-allow-neg
  [opts]
  (ygen/workload-with-op-index (bank/test {:negative-balances? true})))
