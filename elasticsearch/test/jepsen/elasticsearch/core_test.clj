(ns jepsen.elasticsearch.core-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [jepsen.elasticsearch [core       :as e]
                                  [dirty-read :as dirty-read]]
            [jepsen [core :as jepsen]
                    [store :as store]]))

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


(deftest dirty-read-test
  (let [ts (->> #(jepsen/run! (dirty-read/test
                                {:tarball "https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/5.0.0-alpha5/elasticsearch-5.0.0-alpha5.tar.gz"
                                 :concurrency 30
                                 :time-limit 100}))
                repeatedly
                (take 100)
                seq)] ; memoize
    (pprint {:divergence  (divergence ts)
             :dirty       (dirty ts)
             :some-lost   (some-lost ts)
             :lost        (lost ts)})))
