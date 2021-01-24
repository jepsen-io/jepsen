(ns jecci.common.txn
  (:require [jecci.interface.client :as ic]
            [jepsen.client :as client]))


(defn client
  "Constructs a transactional client. Opts are:

    :val-type     An SQL type string, like \"int\", for the :val field schema.
    :table-count  How many tables to stripe records over."
  [opts]
  (ic/gen-Client nil
    (:val-type opts "int")
    (:table-count opts 7)))
