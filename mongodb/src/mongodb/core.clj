(ns mongodb.core
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [debug info warn]]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh timeout]]
                    [control   :as c]
                    [checker   :as checker]
                    [model     :as model]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.control [net :as net]
                            [util :as net/util]]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [knossos.core :as knossos]))

(defn db []
  (reify db/DB
    (setup! [_ test node])

    (teardown! [_ test node])))

(defn document-cas-test
  "Document-level compare and set."
  []
  (assoc tests/noop-test
         :name      "mongodb document cas"
         :os        debian/os
         :db        (db)
;         :client    (document-cas-client)
         :model     (model/cas-register)
         :checker   checker/linearizable
         :nemesis   (nemesis/partition-random-halves)
;         :generator (gen/phases)
  ))
