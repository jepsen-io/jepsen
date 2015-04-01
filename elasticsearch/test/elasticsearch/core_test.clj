(ns elasticsearch.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [elasticsearch.core :refer :all]
            [jepsen [core :as jepsen]
                    [report :as report]]))

;(deftest register-test
;  (let [test (run!
;               (assoc
;                 noop-test
;                 :name      "elasticsearch"
;                 :os        debian/os
;                 :db        db
;                 :client    (cas-set-client)
;                 :model     (model/set)
;                 :checker   (checker/compose {:html timeline/html
;                                              :set  checker/set})
;                 :nemesis   (nemesis/partitioner nemesis/bridge)
;                 :generator (gen/phases
;                              (->> (range)
;                                   (map (fn [x] {:type  :invoke
;                                                 :f     :add
;                                                 :value x}))
;                                   gen/seq
;                                   (gen/stagger 1/10)
;                                   (gen/delay 1)
;                                   (gen/nemesis
;                                     (gen/seq
;                                       (cycle [(gen/sleep 60)
;                                               {:type :info :f :start}
;                                               (gen/sleep 300)
;                                               {:type :info :f :stop}])))
;                                   (gen/time-limit 600))
;                              (gen/nemesis
;                                (gen/once {:type :info :f :stop}))
;                              (gen/clients
;                                (gen/once {:type :invoke :f :read})))))]
;    (is (:valid? (:results test)))
;    (pprint (:results test))))

(deftest create
  (let [test (jepsen/run! (create-test))]
    (is (:valid? (:results test)))
    (report/to "report/history.edn" (pprint (:history test)))
    (report/to "report/set.edn" (pprint (:set (:results test))))))
