(ns jepsen.lazyfs-test
  "Tests for the lazyfs write-losing filesystem"
  (:require [clojure [data :refer [diff]]
                     [pprint :refer [pprint]]
                     [string :as str]
                     [test :refer :all]]
            [clojure.java.io :as io]
            [clojure.test.check [clojure-test :refer :all]
                                [generators :as g]
                                [properties :as prop]
                                [results :refer [Result]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [common-test :refer [quiet-logging]]
                    [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [generator :as gen]
                    [lazyfs :as lazyfs]
                    [nemesis :as nem]
                    [os :as os]
                    [tests :as tests]
                    [util :as util :refer [pprint-str
                                           timeout]]]
            [jepsen.control [core :as cc]
                            [util :as cu]
                            [sshj :as sshj]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

; (use-fixtures :once quiet-logging)

(defrecord FileSetClient [dir file node]
  client/Client
  (open! [this test node]
    (assoc this
           :file (str dir "/set")
           :node node))

  (setup! [this test]
    this)

  (invoke! [this test op]
    (-> (c/on-nodes test [node]
                    (fn [_ _]
                      (case (:f op)
                        :add  (do (c/exec :echo (str (:value op) " ") :>> file)
                                  (assoc op :type :ok))
                        :read (let [vals (-> (c/exec :cat file)
                                             (str/split #"\s+")
                                             (->> (remove #{""})
                                                  (mapv parse-long)))]
                                (assoc op :type :ok, :value vals)))))
        (get node)))

  (teardown! [this test])

  (close! [this test]))

(defn file-set-client
  "Writes a set to a single file on one node, in the given directory."
  [dir]
  (map->FileSetClient {:dir dir}))

(deftest ^:integration file-set-test
  (let [dir    "/tmp/jepsen/file-set-test"
        lazyfs (lazyfs/lazyfs dir)
        test (assoc tests/noop-test
                    :name      "lazyfs file set"
                    :os        debian/os
                    :db        (lazyfs/db lazyfs)
                    :client    (file-set-client dir)
                    :nemesis   (lazyfs/nemesis lazyfs)
                    :generator (gen/phases
                                 (->> (range)
                                      (map (fn [x] {:f :add, :value x}))
                                      (gen/delay 1/100)
                                      (gen/nemesis
                                        (->> {:type :info
                                              :f    :lose-unfsynced-writes
                                              :value ["n1"]}
                                             repeat
                                             (gen/delay 1/2)))
                                      (gen/time-limit 5))
                                 (gen/clients {:f :read}))
                    :checker   (checker/set)
                    :nodes     ["n1"])
        test (jepsen/run! test)]
    ;(pprint (:history test))
    ;(pprint (:results test))
    (is (false? (:valid? (:results test))))
    (is (pos? (:ok-count (:results test))))
    (is (pos? (:lost-count (:results test))))))
