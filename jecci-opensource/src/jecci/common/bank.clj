(ns jecci.common.bank
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen
             [client :as client]
             [generator :as gen]
             [checker :as checker]]
            [jepsen.tests.bank :as bank]
            [knossos.op :as op]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer :all]
            [jecci.interface.client :as ic]))



(defn workload
  [opts]
  (info "setting up BankClient")
  (assoc (bank/test)
    :client (ic/gen-BankClient nil)))

; One bank account per table
(defn multitable-workload
  [opts]
  (assoc (workload opts)
    :client (ic/gen-MultiBankClient nil (atom false))))
