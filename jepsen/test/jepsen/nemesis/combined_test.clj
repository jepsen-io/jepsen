(ns jepsen.nemesis.combined-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [common-test :refer [quiet-logging]]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util]
                    [generator :as gen]]
            [jepsen.generator [interpreter :as interpreter]
                              [interpreter-test :as it]]
            [jepsen.nemesis.combined :refer :all]))

(use-fixtures :once quiet-logging)

(defn first-primary-db
  "A database whose primary is always \"n1\""
  []
  (reify db/DB
    (setup! [this test node]
      )

    (teardown! [this test node]
      )

    db/Primary
    (primaries [this test]
      ["n1"])))

(deftest partition-package-gen-test
  (let [check-db (fn [db primaries?]
                   (let [pkg (partition-package {:faults   #{:partition}
                                                 :interval 3/100
                                                 :db       db})
                         n   10 ; Op count
                         gen (gen/nemesis (gen/limit n (:generator pkg)))
                         test (assoc it/base-test
                                     :name       "nemesis.combined partition-package-gen-test"
                                     :client     (it/ok-client)
                                     :nemesis    (it/info-nemesis)
                                     :generator  gen)
                         ; Generate some ops
                         h   (:history (jepsen/run! test))]
                     ; Should be alternating start/stop ops
                     (is (= (take (* 2 n)
                                  (cycle [:start-partition :start-partition
                                          :stop-partition  :stop-partition]))
                            (map :f h)))
                     ; Ensure we generate valid target values
                     (let [targets (cond-> #{:one :minority-third :majority
                                             :majorities-ring}
                                     primaries? (conj :primaries))]
                       (is (->> (filter (comp #{:stop-partition} :f) h)
                                (map :value)
                                (every? nil?)))
                       (is (->> (filter (comp #{:start-partition} :f) h)
                                (map :value)
                                (every? targets))))))]
    (testing "no primaries"
      (check-db db/noop false))

    (testing "primaries"
      (check-db (first-primary-db) true))))
