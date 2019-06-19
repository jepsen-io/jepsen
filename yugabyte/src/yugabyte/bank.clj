(ns yugabyte.bank
  "Simulates transfers between bank accounts"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer [debug info warn]]
            [jepsen.tests.bank :as bank]))

(defn workload
  [opts]
  (bank/test))

(defn workload-allow-neg
  [opts]
  (bank/test {:negative-balances? true}))
