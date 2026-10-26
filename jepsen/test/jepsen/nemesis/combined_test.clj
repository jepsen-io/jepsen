(ns jepsen.nemesis.combined-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [common-test :refer [quiet-logging]]
                    [core :as jepsen]
                    [db :as db]
                    [generator :as gen]
                    [random :as rand]
                    [util :as util]]
            [jepsen.generator [interpreter :as interpreter]
                              [interpreter-test :as it]
                              [test :as gt]]
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

    db/Kill
    (kill! [this test node] :killed)
    (start! [this test node] :started)

    db/Pause
    (pause! [this test node] :pause)
    (resume! [this test node] :resume)

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

(deftest package-gen-test
  (rand/with-seed 2 ; nemesis-packages will make some random choices up front
    (let [packages (nemesis-packages {:faults #{:partition :kill :clock}
                                      :db     (first-primary-db)
                                      :interval 2/1000000})
          h (->> (package-gen 10/1000000 packages)
                 gen/nemesis
                 (gen/limit 50)
                 gt/perfect*
                 ; We only care about the invocations (which are :info for nemeses)
                 (partition 1 2)
                 (map first))]
      (is (= [
              [0 :start-partition]
              [2118 :stop-partition]
              [4470 :start-partition]
              [8124 :stop-partition]
              [10719 :start-partition]
              ; Shift to clock/partition
              [10729 :stop-partition]
              [10739 :check-clock-offsets]
              [11101 :stop-partition]
              [16167 :bump-clock]
              [16545 :start-partition]
              [17602 :stop-partition]
              [18332 :reset-clock]
              [18346 :reset-clock]
              [18550 :bump-clock]
              [19605 :start-partition]
              [19965 :reset-clock]
              [21196 :stop-partition]
              ; Shift to kill/clock
              [21206 :reset-clock]
              [21216 :stop-partition]
              [21226 :kill]
              [22380 :strobe-clock]
              [23380 :start]
              [23498 :bump-clock]
              [25100 :strobe-clock]
              [25683 :bump-clock]
              [25747 :reset-clock]
              [27028 :kill]
              [27300 :strobe-clock]
              [27486 :start]
              [28816 :kill]
              [29100 :start]
              [29297 :kill]
              [29353 :reset-clock]
              [30159 :start]
              ; Shift to nothing
              [30169 :start]
              [30179 :reset-clock]
              ; Shift to clock
              [40000 :strobe-clock]
              [40920 :bump-clock]
              [45256 :reset-clock]
              [51297 :bump-clock]
              ; Shift to kill/clock/partition
              [51307 :reset-clock]
              [51317 :kill]
              [51327 :start-partition]
              [52627 :start]
              [55785 :kill]
              [55918 :stop-partition]
              [58372 :bump-clock]
              [58706 :reset-clock]
              [59290 :start]
              [62075 :kill]
              ]
             ; We really just want to see the balance of ops shifting over time
             (map (juxt :time :f) h))))))
