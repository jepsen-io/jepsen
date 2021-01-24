(ns jecci.common.register
  "Single atomic register test"
  (:refer-clojure :exclude [test read])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]
             [util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.linearizable-register :as lr]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [jecci.common.basic :as basic]
            [knossos.model :as model]
            [jecci.interface.client :as ic]))


(defn workload
  [opts]
  (let [w (lr/test (assoc opts :model (model/cas-register 0)))]
    (-> w
      (assoc :client (ic/gen-AtomicClient nil))
      (update :generator (partial gen/stagger 1/10)))))
