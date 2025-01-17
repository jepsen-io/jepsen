(ns jepsen.crate-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [jepsen.core :as jepsen]
            [jepsen.crate.core :refer :all]
            [jepsen.crate.lost-updates :as lost-updates]
            [jepsen.crate.dirty-read :as dirty-read]
            [jepsen.store :as store]))

;(deftest a-test
;  (jepsen/run! (an-test {})))

;(deftest lost-updates-test
;  (jepsen/run! (lost-updates/test {})))

(defn find-case
  "Given a predicate and a sequence of tests, finds the analysis file for the
  first test satisfying predicate."
  [pred ts]
  (when-let [t (first (filter pred ts))]
    (.getPath (store/path t "results.edn"))))

(defn some-lost
  "Any lost writes in a sequence of tests?"
  [ts]
  (find-case (comp seq :some-lost :dirty-read :results) ts))

(defn lost
  [ts]
  (find-case (comp seq :lost :dirty-read :results) ts))

(defn dirty
  "Any dirty reads in a sequence of tests?"
  [ts]
  (find-case (comp seq :dirty :dirty-read :results) ts))

(defn divergence
  "Any nodes disagree in a sequence of tests?"
  [ts]
  (find-case (comp not :nodes-agree? :dirty-read :results) ts))

(deftest all-test
  "For various combinations of elasticsearch vs crate reads, writes, and strong
  reads, perform a series of tests with those settings, looking for node
  divergence, dirty reads, lost updates, and partially lost updates. Prints out
  a map of test names to those behaviors to the results file for details
  showing that particular kind of error."
  (->> [#{:read :write :strong-read}
        #{:read :write}
        #{:read :strong-read}
        #{:write :strong-read}
        #{:read}
        #{:write}
        #{:strong-read}
        #{}]
       (map (fn [es-ops]
              (let [ts (take 10 (repeatedly
                                  #(jepsen/run!
                                     (dirty-read/test
                                       {:es-ops      es-ops
                                        :tarball "https://cdn.crate.io/downloads/releases/nightly/crate-latest.tar.gz"
                                        :concurrency 30
                                        :time-limit 100}))))]
                [(:name (first ts))
                 {:divergence  (divergence ts)
                  :dirty       (dirty ts)
                  :some-lost   (some-lost ts)
                  :lost        (lost ts)}])))
       (into (sorted-map))
       pprint))
