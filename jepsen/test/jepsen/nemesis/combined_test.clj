(ns jepsen.nemesis.combined-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [db :as db]
                    [util :as util]
                    [generator :as gen]]
            [jepsen.generator [interpreter :as interpreter]
                              [interpreter-test :as it]]
            [jepsen.nemesis.combined :refer :all]))

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
  (let [pkg (partition-package {:faults   #{:partition}
                                :interval 3/10
                                :db       (first-primary-db)})
        gen (gen/nemesis (gen/limit 5 (:generator pkg)))
        test (assoc it/base-test
                    :client     (it/ok-client)
                    :nemesis    (it/info-nemesis)
                    :generator  gen)
        ; Interpreted
        h   (util/with-relative-time (interpreter/run! test))]
    (is (= (take 10 (cycle [:start-partition :start-partition
                            :stop-partition  :stop-partition]))
           (map :f h)))))
