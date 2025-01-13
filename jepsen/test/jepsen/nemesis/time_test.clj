(ns jepsen.nemesis.time-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [core :as jepsen]
                    [common-test :refer [quiet-logging]]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.nemesis.time :as nt]))

(use-fixtures :once quiet-logging)

(deftest ^:integration bump-clock-test
  ; This isn't going to work on containers, but I at least want to test that it
  ; uploads and compiles the binary.
  (let [test (assoc tests/noop-test
                    :name      "bump-clock-test"
                    :nemesis   (nt/clock-nemesis)
                    :generator (gen/nemesis
                                 (gen/limit 1 nt/bump-gen)))
        test' (jepsen/run! test)
        h (:history test')]
    (is (= 2 (count h)))
    ; If you ever run these tests on actual nodes where you CAN change the
    ; clock, you'll want an alternative condition here.
    (is (re-find #"clock_settime: Operation not permitted"
                 (:err (:data (:exception (h 1))))))))
