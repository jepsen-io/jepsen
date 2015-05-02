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

(defn run-set-test!
  "Runs a test around set creation and dumps some results to the report/ dir"
  [t]
  (let [test (jepsen/run! t)]
    (or (is (:valid? (:results test)))
        (println (:error (:results test))))
    (report/to (str "report/" (:name test) "/history.edn")
               (pprint (:history test)))
    (report/to (str "report/" (:name test) "/set.edn")
               (pprint (:set (:results test))))))

(deftest create-isolate-primaries
  (run-set-test! (create-isolate-primaries-test)))

(deftest create-pause
  (run-set-test! (create-pause-test)))

(deftest create-bridge
  (run-set-test! (create-bridge-test)))

(deftest create-crash
  (run-set-test! (create-crash-test)))
