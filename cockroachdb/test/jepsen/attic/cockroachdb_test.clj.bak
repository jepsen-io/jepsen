(ns jepsen.system.cockroachdb-test
  (:use jepsen.system.cockroachdb
        jepsen.core
        jepsen.core-test
        clojure.test
        clojure.pprint)
  (:require [clojure.string   :as str]
            [jepsen.util      :as util]
            [jepsen.os.debian :as debian]
            [jepsen.checker   :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.model     :as model]
            [jepsen.generator :as gen]
            [jepsen.nemesis   :as nemesis]
            [jepsen.store     :as store]
            [jepsen.report    :as report]))

(deftest write-test
  (let [test (run!
               (assoc
                 noop-test
                 :nodes     [:n5]
                 :name      "cockroachdb"
                 :os        debian/os
                 :db        (db)
                 :client    (roach-client)
                 :model     (model/cas-register)
                 :checker   (checker/compose {:html   timeline/html
                                              :linear checker/linearizable})
                 :nemesis   (nemesis/partition-random-halves)
                 :generator (gen/phases
                              (->> (range)
                                   (map (fn [x] {:type  :invoke
                                                 :f     :write
                                                 :value x}))
                                   gen/seq
                                   (gen/nemesis
                                     (gen/seq
                                       (cycle
                                         [(gen/sleep 30)
                                          {:type :info :f :start}
                                          (gen/sleep 200)
                                          {:type :info :f :stop}])))
                                   (gen/time-limit 400))
                              (gen/nemesis
                                (gen/once {:type :info :f :stop}))
                              (gen/clients
                                (gen/once {:type :invoke :f :read})))))]
    (is (:valid? (:results test)))
    (report/linearizability (:linear (:results test)))))
