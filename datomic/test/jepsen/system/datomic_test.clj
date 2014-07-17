(ns jepsen.system.datomic-test
  (:use jepsen.system.datomic
        jepsen.core
        jepsen.tests
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

(deftest register-test
  (let [test (run!
               (assoc
                 noop-test
                 :name      "datomic-register"
                 :os        debian/os
                 :db        db
;                 :client
                 :checker   (checker/compose {:html   timeline/html
                                              :linear checker/linearizable})
                 :model     (model/cas-register)
                 :nemesis   (nemesis/partition-random-halves)
                 :generator (gen/phases
                              (->> (gen/seq
                                     (cycle [{:type :invoke :f :acquire}
                                             {:type :invoke :f :release}]))
                                gen/each
                                (gen/delay 1)
                                (gen/nemesis
                                  (gen/seq
                                    (cycle [(gen/sleep 5)
                                            {:type :info :f :start}
                                            (gen/sleep 5)
                                            {:type :info :f :stop}])))
                                (gen/time-limit 0)))))]
    (is (:valid? (:results test)))
    (report/linearizability (:linear (:results test)))))
